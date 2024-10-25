# Databricks notebook source
import requests
from bs4 import BeautifulSoup

def get_all_pdf_links(url: str) -> list:
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    urls = []
    for link in soup.find_all('a'):
        current_link = link.get('href')
        if current_link.endswith('pdf'):
            urls.append(current_link)
    return urls

# COMMAND ----------

from requests.exceptions import MissingSchema

def download_pdfs(url: str, to: str):

    file_path = f"{to}/{url.split('/')[-1]}"
    with open(file_path, "wb") as file:
        try:
            response = requests.get(url)
            file.write(response.content)
        except MissingSchema as e:
            response = requests.get(f"https://www.databricks.com/{url}")
            file.write(response.content)
    return file_path

# COMMAND ----------

import re
import io
from pypdf import PdfReader
from pyspark.sql.pandas.functions import pandas_udf
from typing import Iterator, List
import pandas as pd
from pyspark.sql.functions import col

def clean(txt):
    txt = re.sub(r"\n", "", txt)
    return re.sub(r" ?\.", ".", txt)

def parse_and_clean_one_pdf(b: bytes) -> str:
    pdf = io.BytesIO(b)
    reader = PdfReader(pdf)
    chunks = [page_content.extract_text(extraction_mode="layout") for page_content in reader.pages]
    return "\n".join([clean(text) for text in chunks])

@pandas_udf("string")
def parse_and_clean_pdfs_udf(
        batch_iter: Iterator[pd.Series],) -> Iterator[pd.Series]:
    for series in batch_iter:
        yield series.apply(parse_and_clean_one_pdf)

def parse_pdfs(volume_path: str):
    df = (spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").load(volume_path).select("*", "_metadata").repartition(20))
    return df.select(col("_metadata.file_name"), col("path"), parse_and_clean_pdfs_udf("content").alias("text"))

# COMMAND ----------

import time

def endpoint_exists(vsc, vs_endpoint_name):
  try:
    return vs_endpoint_name in [e['name'] for e in client.list_endpoints().get('endpoints', [])]
  except Exception as e:
    #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
    if "REQUEST_LIMIT_EXCEEDED" in str(e):
      print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. The demo will consider it exists")
      return True
    else:
      raise e

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

def index_exists(vsc, endpoint_name, index_full_name):
    try:
        vsc.get_index(endpoint_name, index_full_name).describe()
        return True
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False
    
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

import json

def read_json_file(json_file: str) -> dict:

  with open(json_file) as f:
    d = json.load(f)
    return d

# COMMAND ----------

get_schema = lambda x: sat_schema if x == "sat" else schema
