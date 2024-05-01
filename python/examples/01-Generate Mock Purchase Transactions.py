# Databricks notebook source
catalog_name = "REPLACE_ME"
schema_name = "REPLACE_ME"

# COMMAND ----------

import random
import datetime

def generate_sample_data():
  """Generates mock transaction data that randomly adds bad data"""
  
  # randomly generate bad data
  if bool(random.getrandbits(1)):
    appl_id = None
    acct_no = None
    event_ts = None
    cstone_last_updatetm = None
  else:
    appl_id = random.randint(1000000, 9999999)
    acct_no = random.randint(1000000000000000, 9999999999999999)
    event_ts = datetime.datetime.now()
    cstone_last_updatetm = datetime.datetime.now()

  # randomly generate an MCC description
  categories = ["dining", "transportation", "merchendise", "hotels", "airfare", "grocery stores/supermarkets/bakeries"]
  random_index = random.randint(0, len(categories)-1)
  category = categories[random_index]

  # randomly generate a transaction price
  price = round(random.uniform(1.99, 9999.99), 2)

  data = [
    (appl_id, acct_no, event_ts, category, price, cstone_last_updatetm)
  ]
  df = spark.createDataFrame(data,
                             "appl_id int, acct_no long, event_ts timestamp, category string, price float, cstone_last_updatetm timestamp")
  return df

# COMMAND ----------

spark.sql(f"create schema if not exists {catalog_name}.{schema_name}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.purchase_transactions_bronze
(appl_id int, acct_no long, event_ts timestamp, category string, price float, cstone_last_updatetm timestamp)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

df = generate_sample_data()
df.write.insertInto(f"{catalog_name}.{schema_name}.purchase_transactions_bronze")

# COMMAND ----------


