# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Singleton

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession, DataFrame
from typing import List


class SparkSingleton:
    """A singleton class which returns one Spark instance"""
    __instance = None

    @classmethod
    def get_instance(cls):
        """Create a Spark instance.
        :return: A Spark instance
        """
        return (SparkSession.builder
                .appName("DataFrame Rules Engine")
                .getOrCreate())

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule Types

# COMMAND ----------

class RuleType:

    ValidateExpr = "expr"
    ValidateBounds = "bounds"
    ValidateNumerics = "validNumerics"
    ValidateStrings = "validStrings"
    ValidateDateTime = "validDateTime"
    ValidateComplex = "complex"

# COMMAND ----------

# MAGIC %md
# MAGIC # Structures class

# COMMAND ----------

class Bounds:

    def __init__(self, lower, upper,
                 lowerInclusive = False,
                 upperInclusive = False):
        self.lower = lower
        self.upper = upper
        self.lowerInclusive = lowerInclusive
        self.upperInclusive = upperInclusive
        self._spark = SparkSingleton.get_instance()
        self._jBounds = self._spark._jvm.com.databricks.labs.validation.utils.Structures.Bounds(lower, upper, lowerInclusive, upperInclusive)

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

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule class

# COMMAND ----------

class Rule:
    """
    Definition of a rule
    """
    def __init__(self,
                 rule_name: str,
                 column: pyspark.sql.Column,
                 boundaries: Bounds = None,
                 valid_expr: pyspark.sql.Column = None,
                 valid_strings: List[str] = None,
                 valid_numerics = None,
                 ignore_case: bool = False,
                 invert_match: bool = False):

        self._spark = SparkSingleton.get_instance()
        self._column = column
        self._boundaries = boundaries
        self._valid_expr = valid_expr
        self._valid_strings = valid_strings
        self._valid_numerics = valid_numerics
        self._is_implicit_bool = False

        # Determine the Rule type by parsing the input arguments
        if valid_strings is not None and len(valid_strings) > 0:
            j_valid_strings = Helpers.to_java_array(valid_strings, self._spark._sc)
            self._jRule = self._spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc,
                                                                                     j_valid_strings,
                                                                                     ignore_case, invert_match)
            self._rule_type = RuleType.ValidateStrings

        elif valid_numerics is not None and len(valid_numerics) > 0:
            j_valid_numerics = Helpers.to_java_array(valid_numerics, self._spark._sc)
            self._jRule = self._spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name,
                                                                                     column._jc,
                                                                                     j_valid_numerics)
            self._rule_type = RuleType.ValidateNumerics
        else:
            self._jRule = self._spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc)
            self._is_implicit_bool = True
            self._rule_type = RuleType.ValidateExpr

    def to_string(self):
        return self._jRule.toString()

    def boundaries(self):
        return self._boundaries

    def valid_numerics(self):
        return self._valid_numerics

    def valid_strings(self):
        return self._valid_strings

    def valid_expr(self):
        return self._valid_expr

    def is_implicit_bool(self):
        return self._jRule.implicitBoolean

    def ignore_case(self):
        return self._jRule.ignoreCase

    def invert_match(self):
        return self._jRule.invertMatch

    def rule_name(self):
        return self._jRule.ruleName

    def is_agg(self):
        return self._jRule.isAgg

    def input_column_name(self):
        return self._jRule.inputColumnName

    def rule_type(self):
        return self._rule_type

    def to_java(self):
        return self._jRule


# COMMAND ----------

# MAGIC %md
# MAGIC # RuleSet class

# COMMAND ----------

class RuleSet():

    def __init__(self, df):
        self.spark = SparkSingleton.get_instance()
        self._df = df
        self._jdf = df._jdf
        self._jRuleSet = self.spark._jvm.com.databricks.labs.validation.RuleSet.apply(self._jdf)

    def add(self, rule):
        self._jRuleSet.add(rule.to_java())

    def get_df(self):
        return self._df

    def to_java(self):
        return self._jRuleSet

    def validate(self):
        validation_results = self._jRuleSet.validate(1)
        return validation_results
      
    def get_complete_report(self):
        jCompleteReport = self._jRuleSet.getCompleteReport()
        return DataFrame(jCompleteReport, self.spark._sc)
      
    def get_summary_report(self):
        jSummaryReport = self._jRuleSet.getSummaryReport()
        return DataFrame(jSummaryReport, self.spark._sc)

# COMMAND ----------


