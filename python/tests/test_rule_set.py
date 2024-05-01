import unittest

from src.databricks import RuleSet, Rule
from tests.local_spark_singleton import SparkSingleton

import pyspark.sql.functions as F


def valid_date_udf(ts_column):
    return ts_column.isNotNull() & F.year(ts_column).isNotNull() & F.month(ts_column).isNotNull()


class TestRuleSet(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()

    def test_create_ruleset_from_dataframe(self):
        test_data = [
          (1.0, 2.0, 3.0),
          (4.0, 5.0, 6.0),
          (7.0, 8.0, 9.0)
        ]
        test_df = self.spark.createDataFrame(test_data, schema="retail_price float, scan_price float, cost float")
        test_rule_set = RuleSet(test_df)

        # Ensure that the RuleSet DataFrame is set properly
        assert test_rule_set.get_df().exceptAll(test_df).count() == 0

    def test_list_of_strings(self):
        iot_readings = [
            (1001, "zone_a", 50.1),
            (1002, "zone_b", 25.4),
            (1003, "zone_c", None)
        ]
        valid_zones = ["zone_a", "zone_b", "zone_c", "zone_d"]
        df = self.spark.createDataFrame(iot_readings).toDF("device_id", "zone_id", "temperature")
        rule_set = RuleSet(df)

        # Add a list of strings
        valid_zones_rule = Rule("valid_zones", F.col("zone_id"), valid_strings=valid_zones)
        rule_set.add(valid_zones_rule)

        # Ensure that the summary report contains no failed rules
        validation_summary = rule_set.validate().get_summary_report()
        assert validation_summary.where(F.col("failed_rules").isNotNull()).count() == 0

        # Add a row that _should_ fail
        new_iot_reading = [
            (1004, "zone_z", 30.1)
        ]
        new_reading_df = self.spark.createDataFrame(new_iot_reading).toDF("device_id", "zone_id", "temperature")
        combined_df = df.union(new_reading_df)
        new_rule_set = RuleSet(combined_df)
        new_rule_set.add(valid_zones_rule)
        new_validation_summary = new_rule_set.validate().get_summary_report()

        # Ensure that the added reading should fail due to an invalid zone id string
        assert new_validation_summary.where(F.col("failed_rules").isNotNull()).count() == 1

    def test_list_of_numerics(self):
        iot_readings = [
            (1001, "zone_a", 50.1),
            (1002, "zone_b", 25.4),
            (1003, "zone_c", None)
        ]
        valid_device_ids = [1001, 1002, 1003, 1004, 1005]
        df = self.spark.createDataFrame(iot_readings).toDF("device_id", "zone_id", "temperature")
        rule_set = RuleSet(df)

        # Add a list of numerical values
        valid_device_ids_rule = Rule("valid_device_id", F.col("device_id"), valid_numerics=valid_device_ids)
        rule_set.add(valid_device_ids_rule)

        # Ensure that the summary report contains no failed rules
        validation_summary = rule_set.validate().get_summary_report()
        assert validation_summary.where(F.col("failed_rules").isNotNull()).count() == 0

    def test_boolean_rules(self):
        iot_readings = [
            (1001, "zone_a", 50.1),
            (1002, "zone_b", 25.4),
            (1003, "zone_c", None)
        ]
        df = self.spark.createDataFrame(iot_readings).toDF("device_id", "zone_id", "temperature")
        rule_set = RuleSet(df)

        # Add a rule that `device_id` is not null
        not_null_rule = Rule("valid_device_id", F.col("device_id").isNotNull())
        rule_set.add(not_null_rule)

        # Add a rule that `temperature` is > -100.0 degrees
        valid_temp_rule = Rule("valid_temp", F.col("temperature") > -100.0)
        rule_set.add(valid_temp_rule)

        validation_summary = rule_set.validate().get_summary_report()
        assert validation_summary.where(F.col("failed_rules").isNotNull()).count() == 0

    def test_udf_rules(self):
        iot_readings = [
            (1001, "zone_a", 50.1, "2024-04-25"),
            (1002, "zone_b", 25.4, "2024-04-24"),
            (1003, "zone_c", None, "2024-04-24")
        ]
        df = self.spark.createDataFrame(iot_readings).toDF("device_id", "zone_id", "temperature", "reading_date_str")
        df = df.withColumn("reading_date", F.col("reading_date_str").cast("date")).drop("reading_date_str")
        rule_set = RuleSet(df)

        # Ensure that UDFs can be used to validate data quality
        valid_reading_date_rule = Rule("valid_reading_date", valid_date_udf(F.col("reading_date")))
        rule_set.add(valid_reading_date_rule)

        validation_summary = rule_set.validate().get_summary_report()
        assert validation_summary.where(F.col("failed_rules").isNotNull()).count() == 0

    def test_add_rules(self):
        iot_readings = [
            (1001, "zone_a", 50.1),
            (1002, "zone_b", 25.4),
            (1003, "zone_c", None)
        ]
        df = self.spark.createDataFrame(iot_readings).toDF("device_id", "zone_id", "temperature")
        rule_set = RuleSet(df)

        # Test boolean rule
        temp_rule = Rule("valid_temp", F.col("temperature").isNotNull())
        rule_set.add(temp_rule)

        # Ensure that the RuleSet DF can be set/gotten correctly
        rule_set_df = rule_set.get_df()
        assert rule_set_df.count() == 3
        assert "device_id" in rule_set_df.columns
        assert "zone_id" in rule_set_df.columns
        assert "temperature" in rule_set_df.columns

        # Add a list of strings
        valid_zones_rule = Rule("valid_zones", F.col("zone_id"), valid_strings=["zone_a", "zone_b", "zone_c"])
        rule_set.add(valid_zones_rule)

        # Ensure that the summary report contains failed rules
        validation_summary = rule_set.validate().get_summary_report()
        assert validation_summary.where(F.col("failed_rules").isNotNull()).count() == 1

    def tearDown(self):
        self.spark.stop()
