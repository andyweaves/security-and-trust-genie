# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %run ../common/functions

# COMMAND ----------

catalogs = list(filter(None, [x.catalog for x in sql("SHOW CATALOGS").limit(1000).collect()]))

dbutils.widgets.dropdown(name="catalog", defaultValue=catalogs[0], choices=catalogs, label="catalog")
dbutils.widgets.text(name="schema", defaultValue="security_and_trust_genie", label="schema")
dbutils.widgets.text(name="volume", defaultValue="files", label="volume")
dbutils.widgets.text(name="vector_search_endpoint", defaultValue="security_and_trust_genie", label="vector_search_endpoint")
dbutils.widgets.text(name="sat_schema", defaultValue="security_analysis_tool", label="sat_schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
vector_search_endpoint = dbutils.widgets.get("vector_search_endpoint")
sat_schema = dbutils.widgets.get("sat_schema")

sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------


urls = [*get_all_pdf_links("https://www.databricks.com/trust/security-features/best-practices"), "https://www.databricks.com/sites/default/files/2024-09/databricks-ai-security-framework-dasf-whitepaper.pdf"]
for u in urls:
    download_pdfs(u, volume_path)

# COMMAND ----------

df = parse_pdfs(volume_path)
display(df)

# COMMAND ----------

df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(f"{catalog}.{schema}.pdfs")

# COMMAND ----------

sql(f"ALTER TABLE {catalog}.{schema}.pdfs SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient(disable_notice=True)

if not endpoint_exists(client, vector_search_endpoint):
    client.create_endpoint(name=vector_search_endpoint, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(client, vector_search_endpoint)
print(f"Endpoint named {vector_search_endpoint} is ready.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

source_table_fullname = f"{catalog}.{schema}.pdfs"
vs_index_fullname = f"{catalog}.{schema}.vs_index"

if not index_exists(client, vector_search_endpoint, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {vector_search_endpoint}...")
  client.create_delta_sync_index(
    endpoint_name=vector_search_endpoint,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED",
    primary_key="file_name",
    embedding_source_column="text", 
    embedding_model_endpoint_name="databricks-gte-large-en" 
  )

  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(client, vector_search_endpoint, vs_index_fullname)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(client, vector_search_endpoint, vs_index_fullname)
  client.get_index(vector_search_endpoint, vs_index_fullname).sync()

print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# Add comments to:

#{catalog}.{sat_schema}.security_best_practices -> The best practices
#{catalog}.{sat_schema}.security_analysis_tool.security_checks -> The actual check

# COMMAND ----------

# Create example queries...
#   https://github.com/andyweaves/databricks-notebooks/blob/main/notebooks/security_genie/example_sql_queries.sql

# COMMAND ----------

# Create Pandas UDFs...
#   https://github.com/andyweaves/databricks-notebooks/blob/main/notebooks/security_genie/check_ip_acls_against_system_tables.py

# COMMAND ----------

import os
import concurrent.futures

# with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:

#   futures = []
#   for table in tables:
#       futures.append(executor.submit(grant_privilege_to_securable, "SELECT", f"{uc_catalog}.{uc_schema}.{table}", principal))
#   for future in concurrent.futures.as_completed(futures):
#       print(future.result())

# COMMAND ----------

# import os
# import concurrent.futures

# with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:

#   futures = [executor.submit(pii_scanner.scan_and_tag_securable, f"{securable.table_catalog}.{securable.table_schema}.{securable.table_name}", securable.table_type) for securable in all_tables.collect()]
  
#   for future in concurrent.futures.as_completed(futures):

#     result = future.result()
#     if isinstance(result, pd.DataFrame):
#       scan_results = pd.concat([scan_results, result])
#     else:
#       print(result)

# COMMAND ----------

comments = read_json_file(json_file="../resources/comments.json")["tables"]
comments
