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

df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(f"{catalog}.{schema}.trust_center_content")

# COMMAND ----------

sql(f"ALTER TABLE {catalog}.{schema}.trust_center_content SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient(disable_notice=True)

if not endpoint_exists(client, vector_search_endpoint):
    client.create_endpoint(name=vector_search_endpoint, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(client, vector_search_endpoint)
print(f"Endpoint named {vector_search_endpoint} is ready.")

# COMMAND ----------

import databricks.sdk.service.catalog as c

source_table_fullname = f"{catalog}.{schema}.trust_center_content"
vs_index_fullname = f"{catalog}.{schema}.trust_center_content_vs"

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

comments = read_json_file(json_file="../resources/config.json")["comments"]
for c in comments:
    comment = c['comment']
    sql_command = f"COMMENT ON TABLE {c['table'].format(catalog=catalog, schema=get_schema(c['type']))} IS '{comment}'"
    spark.sql(sql_command)

# COMMAND ----------

sql(f"""CREATE OR REPLACE FUNCTION {catalog}.{schema}.security_best_practices()
    RETURNS TABLE
    COMMENT 'Trused asset that can be used to answer questions like: What security best practices should I follow for Databricks?'
    RETURN 
        SELECT 
        id, 
        check_id, 
        category, 
        check, 
        severity, 
        recommendation, 
        doc_url, 
        logic, 
        api 
        FROM {catalog}.{sat_schema}.security_best_practices
        ORDER BY id ASC
    """)

# COMMAND ----------

sql(f"""CREATE OR REPLACE FUNCTION {catalog}.{schema}.security_posture()
    RETURNS TABLE
    COMMENT 'Trusted asset that can be used to answer questions like: How does my account compare against security best practices? Which security best practices am I following?'
    RETURN 
    SELECT
        sc.workspaceid,
        sbp.severity,
        sbp.check_id,
        sbp.category,
        check,
        CASE
        WHEN sc.score = 0 THEN TRUE
        WHEN sc.score = 1 THEN FALSE
        ELSE NULL
        END AS is_implemented,
        sbp.recommendation,
        sbp.doc_url,
        sc.additional_details
        FROM {catalog}.{sat_schema}.security_checks sc
        LEFT JOIN {catalog}.{sat_schema}.security_best_practices sbp ON sc.id = sbp.id
        ORDER BY CASE sbp.severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 WHEN 'Low' THEN 3 END,
        sc.workspaceid DESC
    """)

# COMMAND ----------

sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.not_implemented_security_best_practices()
RETURNS TABLE
COMMENT 'Trusted asset that can be used to answer questions like: How can I improve my security posture? Which security best practices should I implement next?'
RETURN 
SELECT
    sc.workspaceid,
    sbp.severity,
    sbp.check_id,
    sbp.category,
    check,
    sbp.recommendation,
    sbp.doc_url,
    sc.additional_details
FROM {catalog}.{sat_schema}.security_checks sc
LEFT JOIN {catalog}.{sat_schema}.security_best_practices sbp ON sc.id = sbp.id
WHERE sc.score = 1
ORDER BY 
    CASE sbp.severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 WHEN 'Low' THEN 3 END,
    sc.workspaceid DESC
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.failed_authentication_attempts')(
# MAGIC   num_days INT DEFAULT 90 COMMENT 'The number of days to search for'
# MAGIC ) 
# MAGIC RETURNS TABLE
# MAGIC COMMENT 'Trused asset that can be used to answer questions like: Have there been any failed authentication attempts? Is anyone trying to hack or brute force access to my Databricks account?'
# MAGIC RETURN SELECT 
# MAGIC     WINDOW(event_time, '60 minutes').start AS window_start, 
# MAGIC     WINDOW(event_time, '60 minutes').end AS window_end, 
# MAGIC     source_ip_address, 
# MAGIC     ifnull(user_identity.email, request_params.user) AS username, 
# MAGIC     workspace_id, 
# MAGIC     service_name, 
# MAGIC     action_name, 
# MAGIC     response.status_code, 
# MAGIC     response.error_message, 
# MAGIC     count(*) AS total_events 
# MAGIC     FROM 
# MAGIC     system.access.audit 
# MAGIC     WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC     AND (contains(lower(action_name), 'login') OR contains(lower(action_name), 'auth')) 
# MAGIC     AND response.status_code = 401  
# MAGIC     GROUP BY 
# MAGIC     window_start, window_end, source_ip_address, username, workspace_id, service_name, action_name, response.status_code, response.error_message
# MAGIC     ORDER BY 
# MAGIC     window_end DESC, 
# MAGIC     total_events DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.failed_authorization_attempts')(
# MAGIC   num_days INT DEFAULT 90 COMMENT 'The number of days to search for'
# MAGIC ) 
# MAGIC RETURNS TABLE
# MAGIC COMMENT 'Trused asset that can be used to answer questions like: Have there been any failed authorization attempts? Is anyone trying to elevate their privileges within my Databricks account?'
# MAGIC RETURN SELECT 
# MAGIC     WINDOW(event_time, '60 minutes').start AS window_start,
# MAGIC     WINDOW(event_time, '60 minutes').end AS window_end,
# MAGIC     source_ip_address,
# MAGIC     ifnull(user_identity.email, request_params.user) AS username,
# MAGIC     workspace_id,
# MAGIC     service_name,
# MAGIC     action_name,
# MAGIC     response.status_code,
# MAGIC     collect_set(response.error_message) AS error_messages,
# MAGIC     count(*) AS total_events
# MAGIC     FROM 
# MAGIC     system.access.audit 
# MAGIC     WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC     AND action_name NOT IN ('IpAccessDenied', 'accountIpAclsValidationFailed')
# MAGIC     AND response.status_code = 403
# MAGIC     GROUP BY 
# MAGIC     window_start, window_end, source_ip_address, username, workspace_id, service_name, action_name, response.status_code, response.error_message
# MAGIC     ORDER BY 
# MAGIC     window_end DESC, 
# MAGIC     total_events DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.ip_access_list_failures')(
# MAGIC   num_days INT DEFAULT 90 COMMENT 'The number of days to search for'
# MAGIC ) 
# MAGIC RETURNS TABLE
# MAGIC COMMENT 'Trused asset that can be used to answer questions like: Has anyone tried to access my Databricks account from an untrusted network? Is anyone trying to hack into my Databricks account?'
# MAGIC RETURN SELECT 
# MAGIC   WINDOW(event_time, '60 minutes').start AS window_start,
# MAGIC   WINDOW(event_time, '60 minutes').
# MAGIC   end AS window_end,
# MAGIC   source_ip_address,
# MAGIC   ifnull(user_identity.email, request_params.user) AS username,
# MAGIC   workspace_id,
# MAGIC   service_name,
# MAGIC   action_name,
# MAGIC   response.status_code,
# MAGIC   response.error_message,
# MAGIC   count(*) AS total_events
# MAGIC   FROM system.access.audit 
# MAGIC   WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC   AND action_name IN ('IpAccessDenied', 'accountIpAclsValidationFailed')
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY 
# MAGIC   window_end DESC, 
# MAGIC   total_events DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.repeated_access_to_secrets')(
# MAGIC   num_days INT DEFAULT 90 COMMENT 'The number of days to search for'
# MAGIC ) 
# MAGIC RETURNS TABLE
# MAGIC COMMENT 'Trused asset that can be used to answer questions like: Is anyone trying to repeatedly access secrets within my Databricks account? Are there any signs of credential theft?'
# MAGIC RETURN SELECT 
# MAGIC   WINDOW(event_time, '60 minutes').start AS window_start,
# MAGIC   WINDOW(event_time, '60 minutes').
# MAGIC   end AS window_end,
# MAGIC   source_ip_address,
# MAGIC   ifnull(user_identity.email, request_params.user) AS username,
# MAGIC   workspace_id,
# MAGIC   service_name,
# MAGIC   action_name,
# MAGIC   response.status_code,
# MAGIC   collect_set(
# MAGIC     concat(
# MAGIC       'Scope: ',
# MAGIC       request_params.scope,
# MAGIC       ' Key: ',
# MAGIC       audit.request_params.key
# MAGIC     )
# MAGIC   ) AS secrets_accessed,
# MAGIC   collect_set(response.error_message) AS error_messages,
# MAGIC   count(*) AS total_events
# MAGIC   FROM system.access.audit 
# MAGIC   WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC   AND action_name = 'getSecret'
# MAGIC   AND user_identity.email NOT IN ('System-User')
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY 
# MAGIC   window_end DESC, 
# MAGIC   total_events DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.databricks_workspace_access')(
# MAGIC   num_days INT DEFAULT 90 COMMENT 'The number of days to search for'
# MAGIC ) 
# MAGIC RETURNS TABLE
# MAGIC COMMENT 'Trused asset that can be used to answer questions like: Has anyone from Databricks logged into my account recently?'
# MAGIC RETURN SELECT 
# MAGIC   event_date,
# MAGIC   event_time,
# MAGIC   source_ip_address,
# MAGIC   workspace_id,
# MAGIC   request_params.user,
# MAGIC   request_params.duration,
# MAGIC   request_params.reason,
# MAGIC   response.status_code
# MAGIC   FROM system.access.audit 
# MAGIC   WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC   AND action_name = 'databricksAccess'
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY 
# MAGIC   event_date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.databricks_access_allowed')(
# MAGIC   num_days INT DEFAULT 90 COMMENT 'The number of days to search for'
# MAGIC ) 
# MAGIC RETURNS TABLE
# MAGIC COMMENT 'Trused asset that can be used to answer questions like: Is it possible for Databricks to log in to my account?'
# MAGIC RETURN SELECT 
# MAGIC   event_date,
# MAGIC   event_time,
# MAGIC   workspace_id,
# MAGIC   user_identity.email,
# MAGIC   CASE
# MAGIC     WHEN request_params.workspaceConfValues = 'indefinite' THEN 'No Expiration'
# MAGIC     WHEN cast(request_params.workspaceConfValues AS TIMESTAMP) < event_time THEN 'Disabled Access'
# MAGIC     ELSE 'Enabled Access for ' || INT(
# MAGIC       round(
# MAGIC         cast(
# MAGIC           cast(request_params.workspaceConfValues AS TIMESTAMP) - event_time AS INT
# MAGIC         ) / 3600
# MAGIC       )
# MAGIC     ) || ' Hours'
# MAGIC   END AS set_configuration
# MAGIC   FROM system.access.audit 
# MAGIC   WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC   AND action_name = 'workspaceConfEdit'
# MAGIC   AND request_params.workspaceConfKeys = 'customerApprovedWSLoginExpirationTime'
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY 
# MAGIC   event_time DESC

# COMMAND ----------

# Because it uses ai_query this create function statement can only run on a SQL warehouse

statement = f"""CREATE OR REPLACE FUNCTION {catalog}.{schema}.search_trust_center_content(question STRING COMMENT 'The question to ask')
RETURNS TABLE
COMMENT 'A trusted asset that can be used to search for content from the Databricks Security & Trust Center and then use an LLM to summarize the results.'
RETURN (WITH vs AS (SELECT text, file_name FROM VECTOR_SEARCH(
index => '{catalog}.{schema}.trust_center_content_vs', 
query => question, 
num_results => 1))

SELECT ai_query(
    endpoint => 'databricks-meta-llama-3-1-70b-instruct', 
    request => concat('Given the following information, please answer the following question: ', question, ' information: ', (SELECT text FROM vs))) AS answer, (SELECT file_name FROM vs) AS answer_source);
    """

# COMMAND ----------

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

wh = ws.warehouses.list()[0]
ws.statement_execution.execute_statement(statement=statement, warehouse_id=wh.id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Todo...
# MAGIC
# MAGIC * The functions below need refactoring to account for when a workspace doesn't have IP ACLs configured. In that scenario it considers all access to be outside of the trusted IP range (which may/may not be true, there just isn't a good way to measure it!)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.ipv4_to_long')(ip STRING COMMENT 'The IPv4 address to convert')
# MAGIC -- RETURNS LONG
# MAGIC -- COMMENT 'Converts an IPv4 address to a LONG for faster lookup/searching'
# MAGIC -- RETURN
# MAGIC --   CASE
# MAGIC --     WHEN regexp_count(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+") <> 1 THEN NULL
# MAGIC --     ELSE 
# MAGIC --       mod(BIGINT(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[3]), 256) +
# MAGIC --       mod(BIGINT(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[2]), 256) * 256 +
# MAGIC --       mod(BIGINT(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[1]), 256) * 65536 +
# MAGIC --       mod(BIGINT(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[0]), 256) * 16777216
# MAGIC --   END;

# COMMAND ----------

# sql(f"USE CATALOG {catalog}")
# sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.ipv4_cidr_to_range')(cidr STRING)
# MAGIC -- RETURNS ARRAY<BIGINT>
# MAGIC -- RETURN ARRAY(ipv4_to_long(split(cidr, "/")[0]), ipv4_to_long(split(cidr, "/")[0]) + power(2, 32-int(split(cidr, "/")[1]))-1)

# COMMAND ----------

# from databricks.sdk import WorkspaceClient
# from pyspark.sql.functions import explode, expr

# # This only gets IP ACLs for the current workspace...
# ws = WorkspaceClient()
# ws_ips = [ip_acl for ip_acl in ws.ip_access_lists.list() if ip_acl.enabled and ip_acl.list_type.value == "ALLOW"]

# data = [(ip_acl.label, ip_acl.ip_addresses) for ip_acl in ws_ips]
# data.append( # add private IPs to filter out internal traffic
#     ("private IPs", list(["192.168.0.0/16", "10.0.0.0/8", "72.16.0.0/12"]))
# )

# df = (spark.createDataFrame(data, ["name", "cidrs"])
#       .select(
#         "name", 
#         explode("cidrs").alias("cidr"), 
#         expr("ipv4_cidr_to_range(cidr)").alias("cird_range")))
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.trusted_ip_addresses')(
# MAGIC --   name STRING,
# MAGIC --   cidr STRING,
# MAGIC --   cidr_range ARRAY<BIGINT>
# MAGIC -- )
# MAGIC -- COMMENT 'The `trusted_ip_addresses` table contains a list of IP addresses that are allowed to access your account and workspaces. `private IPs` are automatically considered trusted here in order to filter out internal compute plane traffic. It includes the name, its CIDR notation, and an array of the CIDR range start and end values an ARRAY<LONG> for faster lookups/searching. The data can be used to monitor the list of trusted IP addresses.'

# COMMAND ----------

# df.write.mode("overwrite").insertInto(f"{catalog}.{schema}.trusted_ip_addresses")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.authentication_attempts_from_untrusted_networks')(num_days INT DEFAULT 90 COMMENT 'The number of days to search for')
# MAGIC -- RETURNS TABLE
# MAGIC -- COMMENT 'Trusted asset that can be used to answer questions like: Has anyone tried to login to my account from an untrusted network? Is anyone trying to hack into my Databricks account?'
# MAGIC -- LANGUAGE SQL
# MAGIC -- RETURN SELECT
# MAGIC --   WINDOW(event_time, '60 minutes').start AS window_start,
# MAGIC --   WINDOW(event_time, '60 minutes').end AS window_end,
# MAGIC --   source_ip_address,
# MAGIC --   ifnull(user_identity.email, request_params.user) AS username,
# MAGIC --   workspace_id,
# MAGIC --   service_name,
# MAGIC --   action_name,
# MAGIC --   response.status_code,
# MAGIC --   response.error_message,
# MAGIC --   count(1) AS attempts, 
# MAGIC --   count(1) FILTER (WHERE response.status_code = 200) successful_attempts
# MAGIC --   FROM system.access.audit LEFT ANTI JOIN trusted_ip_addresses 
# MAGIC --   ON ipv4_to_long(source_ip_address) BETWEEN cidr_range[0] AND cidr_range[1]
# MAGIC --   WHERE event_date >= DATE_ADD((SELECT MAX(event_date) FROM system.access.audit), - num_days)
# MAGIC --   AND source_ip_address IS NOT NULL
# MAGIC --   AND source_ip_address NOT IN ('127.0.0.1', '0.0.0.0', '0:0:0:0:0:0:0:1%0', '')
# MAGIC --   AND (action_name IN ('oidcTokenAuthorization') OR action_name LIKE '%login')
# MAGIC --   GROUP BY ALL
# MAGIC --   ORDER BY 
# MAGIC --   window_end DESC,
# MAGIC --   successful_attempts DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT 
# MAGIC -- * 
# MAGIC -- FROM IDENTIFIER(:catalog || '.' || :schema || '.authentication_attempts_from_untrusted_networks')(90)
# MAGIC -- WHERE attempts != successful_attempts
