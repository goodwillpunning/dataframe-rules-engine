import pyspark
from typing import List

from databricks.labs.validation.local_spark_singleton import SparkSingleton


class Bounds:

    def __init__(self, lower, upper,
                 lowerInclusive=False,
                 upperInclusive=False):
        self.lower = lower
        self.upper = upper
        self.lowerInclusive = lowerInclusive
        self.upperInclusive = upperInclusive
        self._spark = SparkSingleton.get_instance()
        self._jBounds = self._spark._jvm.com.databricks.labs.validation.utils.Structures.Bounds(lower, upper,
                                                                                                lowerInclusive,
                                                                                                upperInclusive)

    def validationLogic(self, col):
        jCol = col._jc
        return self._spark._jvm.com.databricks.labs.validation.utils.Structures.Bounds.validationLogic(jCol)


class MinMaxRuleDef:

    def __init__(self,
                 rule_name: str,
                 column: pyspark.sql.Column,
                 bounds: Bounds,
                 by: List[pyspark.sql.Column] = None):
        self.rule_name = rule_name
        self.column = column
        self.bounds = bounds
        self.by = by


class ValidationResults:

    def __init__(self,
                 complete_report: pyspark.sql.DataFrame,
                 summary_report: pyspark.sql.DataFrame):
        self.complete_report = complete_report
        self.summary_report = summary_report

    def get_complete_report(self):
        return self.complete_report

    def get_summary_report(self):
        return self.summary_report
