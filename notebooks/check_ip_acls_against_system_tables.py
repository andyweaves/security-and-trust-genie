# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")

# COMMAND ----------

sql(f"USE CATALOG {dbutils.widgets.get('catalog')}")
sql(f"USE SCHEMA {dbutils.widgets.get('schema')}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace function ipv4_to_long(ip string)
# MAGIC returns long
# MAGIC return
# MAGIC   case
# MAGIC     when regexp_count(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+") <> 1 then null
# MAGIC     else 
# MAGIC       mod(bigint(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[3]), 256) +
# MAGIC       mod(bigint(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[2]), 256) * 256 +
# MAGIC       mod(bigint(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[1]), 256) * 65536 +
# MAGIC       mod(bigint(split(regexp_extract(ip, "\\d+\\.\\d+\\.\\d+\\.\\d+", 0), "\\.")[0]), 256) * 16777216
# MAGIC   end;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace function ipv4_cidr_to_range(cidr string)
# MAGIC returns array<long>
# MAGIC return array(ipv4_to_long(split(cidr, "/")[0]), ipv4_to_long(split(cidr, "/")[0]) + power(2, 32-int(split(cidr, "/")[1]))-1)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import explode, expr

# this only gets IP ACLs for the current workspace...
ws = WorkspaceClient()
ws_ips = [ip_acl for ip_acl in ws.ip_access_lists.list() if ip_acl.enabled and ip_acl.list_type.value == "ALLOW"]

data = [(ip_acl.label, ip_acl.ip_addresses) for ip_acl in ws_ips]
data.append( # add private IPs to filter out internal traffic
    ("private IPs", list(["192.168.0.0/16", "10.0.0.0/8", "72.16.0.0/12"]))
)

dataset = (spark.createDataFrame(data, ["name", "cidrs"])
           .select("name", explode("cidrs").alias("cidr"), expr("ipv4_cidr_to_range(cidr)").alias("cird_range")))

# COMMAND ----------

display(dataset)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE good_ips_list (
# MAGIC   name STRING,
# MAGIC   cidr STRING,
# MAGIC   cidr_range ARRAY<BIGINT>
# MAGIC )
# MAGIC COMMENT 'The `good_ips_list` table contains a list of IP addresses that are allowed to access your account and workspaces. It includes the name, its CIDR notation, and an array of the CIDR range start and end values in numeric format. The data can be used to monitor and update the list of allowed IP addresses, as well as to analyze the distribution of IP addresses and their corresponding CIDR ranges.'

# COMMAND ----------

dataset.write.mode("overwrite").insertInto("good_ips_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE good_ips_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from good_ips_list

# COMMAND ----------

# DBTITLE 1,Requests Coming from IPs Outside The IP ACLs Range
# MAGIC %sql
# MAGIC select source_ip_address, action_name, user_identity, workspace_id, event_date, count(*) from system.access.audit
# MAGIC left anti join good_ips_list on ipv4_to_long(source_ip_address) between cidr_range[0] and cidr_range[1]
# MAGIC where source_ip_address is not null
# MAGIC and source_ip_address not in ('127.0.0.1', '0.0.0.0', '0:0:0:0:0:0:0:1%0', '')
# MAGIC and action_name not in ('IpAccessDenied', 'accountIpAclsValidationFailed')
# MAGIC and event_date >= current_date() - interval 1 week
# MAGIC group by all
# MAGIC order by 5 desc,6 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION auth_attempts_outside_perimeter()
# MAGIC   RETURNS TABLE
# MAGIC   LANGUAGE SQL
# MAGIC   RETURN 
# MAGIC   SELECT
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
# MAGIC   count(1) AS attempts, 
# MAGIC   count(1) FILTER (WHERE response.status_code = 200) successful_attempts
# MAGIC   FROM system.access.audit
# MAGIC   LEFT ANTI JOIN good_ips_list
# MAGIC     ON ipv4_to_long(source_ip_address) BETWEEN cidr_range[0] and cidr_range[1]
# MAGIC   WHERE source_ip_address IS NOT NULL
# MAGIC   AND source_ip_address NOT IN ('127.0.0.1', '0.0.0.0', '0:0:0:0:0:0:0:1%0', '')
# MAGIC   AND (action_name IN ('oidcTokenAuthorization') OR action_name LIKE '%login')
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY 
# MAGIC   window_end DESC,
# MAGIC   successful_attempts DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM auth_attempts_outside_perimeter() 
# MAGIC WHERE attempts != successful_attempts
