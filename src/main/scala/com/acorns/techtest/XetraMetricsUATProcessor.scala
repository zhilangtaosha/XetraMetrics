package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.biggestvolume.{DailyBiggestVolumes, DailyBiggestVolumesUAT}
import com.acorns.techtest.biggestwinner.{DailyBiggestWinners, DailyBiggestWinnersUAT}
import com.acorns.techtest.schema.TradeActivity
import com.acorns.techtest.securityvolume.BiggestVolumes
import com.acorns.techtest.util.{CustomStringUtils, ResourceUtils}
import org.apache.spark.sql.{Dataset, SparkSession}

class XetraMetricsUATProcessor(filePath: String, sparkSession: SparkSession) {

  private val xetraSourceData = new XetraSourceData(filePath, sparkSession)

  def showUATResults(): Unit = {
    val stringJoiner = new StringJoiner("\n")

    val tradeActivities = xetraSourceData.getTradeActivities(sparkSession)
    tradeActivities.cache()

    val tradeActivitiesCount = tradeActivities.count()

    stringJoiner.add(s"Found $tradeActivitiesCount source records.")

    tradeActivities.createOrReplaceTempView("xetra")

    stringJoiner.add(CustomStringUtils.getSampleTitle("source"))
    tradeActivities.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(CustomStringUtils.get30Dashes)
    stringJoiner.add("")
    stringJoiner.add("")

    tradeActivities.unpersist()

    stringJoiner
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}DAILY BIGGEST WINNERS")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestWinnersUAT(tradeActivities))
      .add("")
      .add("")
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}DAILY BIGGEST VOLUMES")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestVolumesUAT(tradeActivities))
      .add("")
      .add("")
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}MOST TRADED STOCK/ETF")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getBiggestVolumesUAT(tradeActivities))
      .add("")
      .add("")

    println(stringJoiner.toString)
  }

  private def getDailyBiggestWinnersUAT(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestWinners = new DailyBiggestWinners(sparkSession)

    val controlSql = ResourceUtils.getTextFromResource("/example-queries/xetra_biggest_winner.sql", "\n")
    val controlDataFrame = sparkSession.sql(controlSql)

    val dailyBiggestWinnersBySessionizing = dailyBiggestWinners.getDailyBiggestWinnersBySessionizing(tradeActivities)
    val dailyBiggestWinnersByJoining = dailyBiggestWinners.getDailyBiggestWinnersByJoining(tradeActivities)

    val dataFrameComparison = new DailyBiggestWinnersUAT(
      sparkSession,
      controlDataFrame,
      dailyBiggestWinnersBySessionizing.toDF(),
      dailyBiggestWinnersByJoining.toDF()
    )

    stringJoiner.add(dataFrameComparison.getComparisonSamples("sessionizing", "joining"))

    stringJoiner.toString
  }

  private def getDailyBiggestVolumesUAT(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)

    val controlSql = ResourceUtils.getTextFromResource("/example-queries/xetra_most_traded_instruments_by_volume.sql", "\n")
    val controlDataFrame = sparkSession.sql(controlSql)

    val dailyBiggestVolumesBySessionizing = dailyBiggestVolumes.getDailyBiggestVolumesBySessionizing(tradeActivities)
    val dailyBiggestVolumesByJoining = dailyBiggestVolumes.getDailyBiggestVolumesByJoining(tradeActivities)

    val dataFrameComparison = new DailyBiggestVolumesUAT(
      sparkSession,
      controlDataFrame,
      dailyBiggestVolumesBySessionizing.toDF(),
      dailyBiggestVolumesByJoining.toDF()
    )

    stringJoiner.add(dataFrameComparison.getComparisonSamples("sessionizing", "joining"))

    stringJoiner.toString
  }

  private def getBiggestVolumesUAT(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)

    val controlSql = ResourceUtils.getTextFromResource("/example-queries/xetra_instruments_with_highest_volume.sql", "\n")
    val controlDataFrame = sparkSession.sql(controlSql)

    val securityVolumes = new BiggestVolumes(sparkSession).getBiggestVolumes(tradeActivities)

    val dataFrameComparison = new BiggestVolumesUAT(
      sparkSession,
      controlDataFrame,
      securityVolumes.toDF()
    )

    stringJoiner.add(dataFrameComparison.getComparisonSamples("sessionizing"))

    stringJoiner.toString
  }
}