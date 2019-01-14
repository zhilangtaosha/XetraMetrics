package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.util.OutputStringUtils
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SecurityVolumeComparison(sparkSession: SparkSession,
                               controlDataFrame: DataFrame,
                               dataFrame1: DataFrame) {

  def getComparisonSamples(label1: String): String = {
    val stringJoiner = new StringJoiner("\n")
    val stopWatch = new StopWatch

    controlDataFrame.cache()
    dataFrame1.cache()

    stopWatch.start()
    val label1Count = dataFrame1.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $label1Count records by $label1 in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.add(OutputStringUtils.getSampleTitle(label1))
    dataFrame1.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    val unionedDataFrames =
      controlDataFrame.withColumn("DataFrameName", lit("control")).toDF()
        .union(dataFrame1.withColumn("DataFrameName", lit(label1)).toDF())

    unionedDataFrames.cache()

    val groupedDataFrame = getGroupedDataFrame(unionedDataFrames)

    groupedDataFrame.cache()
    val diffRecords = groupedDataFrame.count()

    stringJoiner.add(s"There are $diffRecords unique records among the $label1, and control datasets.")

    if (diffRecords > 0) {
      joinDataFrames(unionedDataFrames, groupedDataFrame)
        .sort("uniqueIdentifier", "DataFrameName")
        .limit(10)
        .collect()
        .foreach{row =>
          stringJoiner.add(row.toString)
        }
    }

    controlDataFrame.unpersist()
    dataFrame1.unpersist()
    unionedDataFrames.unpersist()
    groupedDataFrame.unpersist()

    stringJoiner.toString
  }

  private def getGroupedDataFrame(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy("uniqueIdentifier", "SecurityID", "Description", "ImpliedVolume")
      .agg(count("*").as("rowCount"))
      .where("rowCount != 2")
  }

  private def joinDataFrames(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    dataFrame1.as("df1")
      .join(
        dataFrame2,
        Seq("uniqueIdentifier", "SecurityID", "Description", "ImpliedVolume"),
        "inner"
      )
      .select(
        "df1.*"
      )
  }
}