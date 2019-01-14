package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.biggestvolume.{DailyBiggestVolumes, DailyBiggestVolumesComparison}
import com.acorns.techtest.biggestwinner.{DailyBiggestWinners, DailyBiggestWinnersComparison}
import com.acorns.techtest.schema.TradeActivity
import com.acorns.techtest.securityvolume.SecurityVolumes
import com.acorns.techtest.util.{OutputStringUtils, ResourceUtils}
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.{Dataset, SparkSession}

class XetraMetricsJob(filePath: String, sparkSession: SparkSession) {
  import sparkSession.implicits._

  def showMetrics(): Unit = {
    val stopWatch = new StopWatch
    stopWatch.start()

    val stringJoiner = new StringJoiner("\n")

    val tradeActivities = getTradeActivities(sparkSession)
    tradeActivities.cache()

    val tradeActivitiesCount = tradeActivities.count()

    stringJoiner.add(s"Found $tradeActivitiesCount source records.")

    tradeActivities.createOrReplaceTempView("xetra")

    stringJoiner.add(OutputStringUtils.getSampleTitle("source"))
    tradeActivities.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")
    stringJoiner.add("")

    tradeActivities.unpersist()

    stringJoiner
      .add(s"/${OutputStringUtils.repeatChar(29, '*')}")
      .add(s"${OutputStringUtils.repeatChar(5, ' ')}DAILY BIGGEST WINNERS")
      .add(s"${OutputStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestWinnersSample(tradeActivities))
      .add("")
      .add("")
//      .add(s"/${OutputStringUtils.repeatChar(29, '*')}")
//      .add(s"${OutputStringUtils.repeatChar(5, ' ')}DAILY BIGGEST VOLUMES")
//      .add(s"${OutputStringUtils.repeatChar(29, '*')}/")
//      .add("")
//      .add(getDailyBiggestVolumesSample(tradeActivities))
//      .add("")
//      .add("")
//      .add(s"/${OutputStringUtils.repeatChar(29, '*')}")
//      .add(s"${OutputStringUtils.repeatChar(5, ' ')}MOST TRADED STOCK/ETF")
//      .add(s"${OutputStringUtils.repeatChar(29, '*')}/")
//      .add("")
//      .add(getBiggestVolumesSample(tradeActivities))
//      .add("")
//      .add("")

    stringJoiner.add(
      s"This application completed in ${stopWatch.getTime} ms."
    )

    println(stringJoiner.toString)
  }

  private def getDailyBiggestWinnersSample(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestWinners = new DailyBiggestWinners(sparkSession)

    val controlSql = ResourceUtils.getTextFromResource("/example-queries/xetra_biggest_winner.sql", "\n")
    val controlDataFrame = sparkSession.sql(controlSql)

    val dailyBiggestWinnersBySessionizing = dailyBiggestWinners.getDailyBiggestWinnersBySessionizing(tradeActivities)
    val dailyBiggestWinnersByJoining = dailyBiggestWinners.getDailyBiggestWinnersByJoining(tradeActivities)

    val dataFrameComparison = new DailyBiggestWinnersComparison(
      sparkSession,
      controlDataFrame,
      dailyBiggestWinnersBySessionizing.toDF(),
      dailyBiggestWinnersByJoining.toDF()
    )

    stringJoiner.add(dataFrameComparison.getComparisonSamples("sessionizing", "joining"))

    stringJoiner.toString
  }

  private def getDailyBiggestVolumesSample(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)

    val controlSql = ResourceUtils.getTextFromResource("/example-queries/xetra_most_traded_instruments_by_volume.sql", "\n")
    val controlDataFrame = sparkSession.sql(controlSql)

    val dailyBiggestVolumesBySessionizing = dailyBiggestVolumes.getDailyBiggestVolumesBySessionizing(tradeActivities)
    val dailyBiggestVolumesByJoining = dailyBiggestVolumes.getDailyBiggestVolumesByJoining(tradeActivities)

    val dataFrameComparison = new DailyBiggestVolumesComparison(
      sparkSession,
      controlDataFrame,
      dailyBiggestVolumesBySessionizing.toDF(),
      dailyBiggestVolumesByJoining.toDF()
    )

    stringJoiner.add(dataFrameComparison.getComparisonSamples("sessionizing", "joining"))

    stringJoiner.toString
  }

  private def getBiggestVolumesSample(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)

    val controlSql = ResourceUtils.getTextFromResource("/example-queries/xetra_instruments_with_highest_volume.sql", "\n")
    val controlDataFrame = sparkSession.sql(controlSql)

    val securityVolumes = new SecurityVolumes(sparkSession).getSecurityVolumes(tradeActivities)

    val dataFrameComparison = new SecurityVolumeComparison(
      sparkSession,
      controlDataFrame,
      securityVolumes.toDF()
    )

    stringJoiner.add(dataFrameComparison.getComparisonSamples("sessionizing"))

    stringJoiner.toString
  }

  private def getTradeActivities(sparkSession: SparkSession): Dataset[TradeActivity] = {
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", true)
      .csv(s"$filePath/*/*.csv")
      .as[TradeActivity]
  }
}