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

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

example_queries = read_json_file(json_file="../resources/config.json")["example_queries"]
for q in example_queries:

    ws.queries.create(name=q["name"], query=q["query"].format(catalog=catalog, schema=get_schema(q["type"])))

vs_query = f"SELECT ai_query('databricks-meta-llama-3-1-70b-instruct', concat('Given the information provided, please answer the following question: ', :question, ' information: ', (SELECT FIRST(text) FROM VECTOR_SEARCH(index => '{vs_index_fullname}', query => :question, num_results => 1)))) AS answer, FIRST(file_name) AS answer_source FROM VECTOR_SEARCH(index => '{vs_index_fullname}', query => :question, num_results => 1)" 

ws.queries.create(name="Search the Databricks Security & Trust Center content", query=vs_query)

# COMMAND ----------

comments = read_json_file(json_file="../resources/config.json")["comments"]
for c in comments:
    comment = c['comment']
    sql_command = f"COMMENT ON TABLE {c['table'].format(catalog=catalog, schema=get_schema(c['type']))} IS '{comment}'"
    spark.sql(sql_command)

# COMMAND ----------

# Create IP checks!
