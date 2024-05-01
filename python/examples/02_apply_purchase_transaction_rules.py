# Databricks notebook source
# MAGIC %run ./PythonWrapper

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest new Data

# COMMAND ----------

import datetime

starting_time = datetime.datetime.now() - datetime.timedelta(minutes=5)

catalog_name = "REPLACE_ME"
schema_name = "REPLACE_ME"

# COMMAND ----------

# Read table changes from 5 mins ago
df = spark.read.format("delta") \
          .option("readChangeFeed", "true") \
          .option("startingTimestamp", starting_time) \
          .table(f"{catalog_name}.{schema_name}.purchase_transactions_bronze")
purchase_transactions_df = df.select("appl_id", "acct_no", "event_ts", "category", "price", "cstone_last_updatetm")\
       .where("_change_type='insert'")
purchase_transactions_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Rules using Builder Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Rules
# MAGIC
# MAGIC From a DQ rule point of view, we would be looking at following scenarios:
# MAGIC
# MAGIC - **event_ts**: Should have a timestamp for every day (timestamp format doesn’t matter)
# MAGIC - **cstone_last_updatetm**: Should have a timestamp for every day
# MAGIC - **acct_no**: No null values for this column
# MAGIC - **appl_id**: No null values for this column
# MAGIC - **Changes in string length** - for all columns
# MAGIC  

# COMMAND ----------

import pyspark.sql.functions as F

# First, begin by defining your RuleSet by passing in your input DataFrame
myRuleSet = RuleSet(purchase_transactions_df)

# Rule 1 - define a Rule that validates that the `acct_no` is never null
acct_num_rule = Rule("valid_acct_no_rule", F.col("acct_no").isNotNull())
myRuleSet.add(acct_num_rule)

# Rule 2 - add a Rule that validates that the `appl_id` is never null
appl_id_rule = Rule("valid_appl_id", F.col("appl_id").isNotNull())
myRuleSet.add(appl_id_rule)

# COMMAND ----------

# Rules can even be used in conjunction with User-Defined Functions
def valid_timestamp(ts_column):
  return ts_column.isNotNull() & F.year(ts_column).isNotNull() & F.month(ts_column).isNotNull()

# COMMAND ----------

# Rule 3 - enforce a valid `event_ts` timestamp
valid_event_ts_rule = Rule("valid_event_ts_rule", valid_timestamp(F.col("event_ts")))
myRuleSet.add(valid_event_ts_rule)

# Rule 4 - enforce a valid `cstone_last_updatetm` timestamp
valid_cstone_last_updatetm_rule = Rule("valid_cstone_last_updatetm_rule", valid_timestamp(F.col("cstone_last_updatetm")))
myRuleSet.add(valid_cstone_last_updatetm_rule)

# COMMAND ----------

# Rule 5 - validate string lengths
import pyspark.sql.functions as F
import datetime

starting_timestamp = datetime.datetime.now() - datetime.timedelta(minutes=5)
ending_timestamp = datetime.datetime.now() - datetime.timedelta(minutes=1)

# Read table changes from 5 mins ago
df = spark.read.format("delta") \
          .option("readChangeFeed", "true") \
          .option("startingVersion", 0) \
          .option("endingVersion", 10) \
          .table(f"{catalog_name}.{schema_name}.purchase_transactions_bronze")
df_category = df.select("category").where("_change_type='insert'").agg(F.mean(F.length(F.col("category"))).alias("avg_category_len"))
avg_category_len = df_category.collect()[0]['avg_category_len']
print(avg_category_len)

# COMMAND ----------

def valid_category_len(category_column, avg_category_str_len):
  return F.length(category_column) <= avg_category_str_len

# Rule 5 - validate `category` string lengths
valid_str_length_rule = Rule("valid_category_str_length_rule", valid_category_len(F.col("category"), avg_category_len))
myRuleSet.add(valid_str_length_rule)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate Rows

# COMMAND ----------

from pyspark.sql import DataFrame

# Finally, add the Rule to the RuleSet and validate!
summaryReport = myRuleSet.get_summary_report()
completeReport = myRuleSet.get_complete_report()

# Display the summary validation report
display(summaryReport)

# COMMAND ----------

# Display the complete validation report
display(completeReport)

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.purchase_transactions_validated
  (appl_id int, acct_no long, event_ts timestamp, category string, price float, cstone_last_updatetm timestamp, failed_rules array<string>)
  USING DELTA
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

if summaryReport.count() > 0:
  summaryReport.write.insertInto(f"{catalog_name}.{schema_name}.purchase_transactions_validated")
else:
  string_array_type = T.ArrayType(T.StringType())
  purchase_transactions_df \
    .withColumn("failed_rules", F.array(F.array().cast(string_array_type))) \
    .write.insertInto(f"{catalog_name}.{schema_name}.purchase_transactions_validated")

# COMMAND ----------


