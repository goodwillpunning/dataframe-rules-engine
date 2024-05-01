from pyspark.sql import DataFrame

from databricks.labs.validation.local_spark_singleton import SparkSingleton
from databricks.labs.validation.structures import ValidationResults, MinMaxRuleDef


class RuleSet():

    def __init__(self, df):
        self.spark = SparkSingleton.get_instance()
        self._df = df
        self._jdf = df._jdf
        self._jRuleSet = self.spark._jvm.com.databricks.labs.validation.RuleSet.apply(self._jdf)

    def add(self, rule):
        self._jRuleSet.add(rule.to_java())

    def addMinMaxRule(self, minMaxRuleDef):
        pass

    def get_df(self):
        return self._df

    def to_java(self):
        return self._jRuleSet

    def validate(self):
        jValidationResults = self._jRuleSet.validate(1)
        complete_report = DataFrame(jValidationResults.completeReport(), self.spark)
        summary_report = DataFrame(jValidationResults.summaryReport(), self.spark)
        validation_results = ValidationResults(complete_report, summary_report)
        return validation_results
