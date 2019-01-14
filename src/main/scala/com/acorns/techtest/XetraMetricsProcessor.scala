package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.biggestvolume.DailyBiggestVolumes
import com.acorns.techtest.biggestwinner.DailyBiggestWinners
import com.acorns.techtest.securityvolume.BiggestVolumes
import com.acorns.techtest.util.CustomStringUtils
import org.apache.spark.sql.SparkSession

class XetraMetricsProcessor(filePath: String, sparkSession: SparkSession) {

  private val xetraSourceData = new XetraSourceData(filePath, sparkSession)

  def showMetrics(): Unit = {

    val tradeActivities = xetraSourceData.getTradeActivities(sparkSession)

    tradeActivities.cache()

    println(
      new StringJoiner("\n")
        .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
        .add(s"${CustomStringUtils.repeatChar(5, ' ')}DAILY BIGGEST WINNERS")
        .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
        .toString
    )

    val dailyBiggestWinners = new DailyBiggestWinners(sparkSession)
    dailyBiggestWinners.getDailyBiggestWinnersBySessionizing(tradeActivities).show()

    println(
      new StringJoiner("\n")
        .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}DAILY BIGGEST VOLUMES")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
        .toString
    )

    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)
    dailyBiggestVolumes.getDailyBiggestVolumesBySessionizing(tradeActivities).show()

    println(
      new StringJoiner("\n")
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}MOST TRADED STOCK/ETF")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
        .toString
    )

    val biggestVolumes = new BiggestVolumes(sparkSession)
    biggestVolumes.getBiggestVolumes(tradeActivities).show()
  }
}