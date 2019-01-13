package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.util.OutputStringUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameComparison(sparkSession: SparkSession,
                          dataFrame1: DataFrame,
                          dataFrame2: DataFrame) {

  def getComparisonSamples(label1: String, label2: String): String = {
    val stringJoiner = new StringJoiner("\n")

    val label1Diff =
      dataFrame1
        .except(dataFrame2)
        .withColumn("source", lit(label1))
    label1Diff.cache()
    stringJoiner.add(s"Count of records ONLY in $label1 data set: ${label1Diff.count()}")

    val label2Diff =
      dataFrame2
        .except(dataFrame1)
        .withColumn("source", lit(label2))
    label2Diff.cache()
    stringJoiner.add(s"Count of records ONLY in $label2 data set: ${label2Diff.count()}")

    dataFrame1.unpersist()
    dataFrame2.unpersist()

    val TotalDiff =
      label1Diff
        .union(label2Diff)
        .sort("uniqueIdentifier", "source")
    TotalDiff.cache()
    val TotalDiffCount = TotalDiff.count()
    stringJoiner.add(s"Total diff count: $TotalDiffCount")

    label1Diff.unpersist()
    label2Diff.unpersist()

    stringJoiner.add(OutputStringUtils.getSampleTitle("DIFF"))
    TotalDiff.limit(10).collect().foreach{row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    TotalDiff.unpersist()

    stringJoiner.toString
  }
}