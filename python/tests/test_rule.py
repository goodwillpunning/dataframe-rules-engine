import unittest
import pyspark.sql
import pyspark.sql.functions as F

from src.databricks.labs.validation.rule import Rule
from src.databricks.labs.validation.rule_type import RuleType
from tests.local_spark_singleton import SparkSingleton


class TestRule(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()


    def test_string_lov_rule(self):
        """Tests that a list of String values rule can be instantiated correctly."""

        # Ensure that a rule with a list of valid strings can be validated
        building_sites = ["SiteA", "SiteB", "SiteC"]
        building_name_rule = Rule("Building_LOV_Rule", column=F.col("site_name"),
                                  valid_strings=building_sites)

        # Ensure that all attributes are set correctly for Integers
        assert building_name_rule.rule_name() == "Building_LOV_Rule", "Rule name is not set as expected."
        assert building_name_rule.rule_type() == RuleType.ValidateStrings, "The rule type is not set as expected."
        assert not building_name_rule.ignore_case()

    def tearDown(self):
        self.spark.stop()
