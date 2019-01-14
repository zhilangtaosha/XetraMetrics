package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.util.OutputStringUtils
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataFrameComparison(sparkSession: SparkSession,
                                   controlDataFrame: DataFrame,
                                   dataFrame1: DataFrame,
                                   dataFrame2: DataFrame) {

  def getComparisonSamples(label1: String, label2: String): String = {
    val stringJoiner = new StringJoiner("\n")
    val stopWatch = new StopWatch

    controlDataFrame.cache()
    dataFrame1.cache()
    dataFrame2.cache()

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

    stopWatch.start()
    val label2Count = dataFrame2.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $label2Count records by $label2 in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.add(OutputStringUtils.getSampleTitle(label2))
    dataFrame2.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    val unionedDataFrames =
      controlDataFrame.withColumn("DataFrameName", lit("control")).toDF()
        .union(dataFrame1.withColumn("DataFrameName", lit(label1)).toDF())
        .union(dataFrame2.withColumn("DataFrameName", lit(label2)).toDF())

    unionedDataFrames.cache()

    val groupedDataFrame = getGroupedDataFrame(unionedDataFrames)

    groupedDataFrame.cache()
    val diffRecords = groupedDataFrame.count()

    stringJoiner.add(s"There are $diffRecords unique records among the $label1, $label2, and control datasets.")

    if (diffRecords > 0) {
      joinDataFrames(unionedDataFrames, groupedDataFrame)
        .sort("uniqueIdentifier", "DataFrameName")
        .limit(15)
        .collect()
        .foreach{row =>
          stringJoiner.add(row.toString)
        }
    }

    controlDataFrame.unpersist()
    dataFrame1.unpersist()
    dataFrame2.unpersist()
    unionedDataFrames.unpersist()
    groupedDataFrame.unpersist()

    stringJoiner.toString
  }

  protected def getGroupedDataFrame(dataFrame: DataFrame): DataFrame

  protected def joinDataFrames(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame
}