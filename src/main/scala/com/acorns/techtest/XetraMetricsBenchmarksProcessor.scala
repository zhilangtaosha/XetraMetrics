package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.biggestvolume.DailyBiggestVolumes
import com.acorns.techtest.biggestwinner.DailyBiggestWinners
import com.acorns.techtest.schema.TradeActivity
import com.acorns.techtest.securityvolume.BiggestVolumes
import com.acorns.techtest.util.CustomStringUtils
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.{Dataset, SparkSession}

class XetraMetricsBenchmarksProcessor(filePath: String, sparkSession: SparkSession) {

  private val xetraSourceData = new XetraSourceData(filePath, sparkSession)

  def showBenchmarks(): Unit = {
    val stringJoiner = new StringJoiner("\n")
    val stopWatch = new StopWatch
    stopWatch.start()

    val tradeActivities = xetraSourceData.getTradeActivities(sparkSession)

    tradeActivities.cache()

    stringJoiner
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}DAILY BIGGEST WINNERS")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestWinnersBenchmarks(tradeActivities))
      .add("")
      .add("")
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}DAILY BIGGEST VOLUMES")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestVolumesBenchmarks(tradeActivities))
      .add("")
      .add("")
      .add(s"/${CustomStringUtils.repeatChar(29, '*')}")
      .add(s"${CustomStringUtils.repeatChar(5, ' ')}MOST TRADED STOCK/ETF")
      .add(s"${CustomStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getBiggestVolumesBenchmarks(tradeActivities))
      .add("")
      .add("")

    stopWatch.stop()

    stringJoiner.add(s"This application completed in ${stopWatch.getTime} ms.")

    println(stringJoiner.toString)
  }

  private def getDailyBiggestWinnersBenchmarks(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")
    val stopWatch = new StopWatch
    val dailyBiggestWinners = new DailyBiggestWinners(sparkSession)

    stopWatch.reset()
    stopWatch.start()
    dailyBiggestWinners.getDailyBiggestWinnersBySessionizing(tradeActivities).take(1)
    stopWatch.stop()
    stringJoiner.add(s"Sessionizing logic completed in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stopWatch.start()
    dailyBiggestWinners.getDailyBiggestWinnersByJoining(tradeActivities).take(1)
    stopWatch.stop()
    stringJoiner.add(s"Joining logic completed in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.toString
  }

  private def getDailyBiggestVolumesBenchmarks(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")
    val stopWatch = new StopWatch
    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)

    stopWatch.reset()
    stopWatch.start()
    dailyBiggestVolumes.getDailyBiggestVolumesBySessionizing(tradeActivities).take(1)
    stopWatch.stop()
    stringJoiner.add(s"Sessionizing logic completed in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stopWatch.start()
    dailyBiggestVolumes.getDailyBiggestVolumesByJoining(tradeActivities).take(1)
    stopWatch.stop()
    stringJoiner.add(s"Joining logic completed in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.toString
  }

  private def getBiggestVolumesBenchmarks(tradeActivities: Dataset[TradeActivity]): String = {
    val stringJoiner = new StringJoiner("\n")
    val stopWatch = new StopWatch
    val biggestVolumes = new BiggestVolumes(sparkSession)

    stopWatch.reset()
    stopWatch.start()
    biggestVolumes.getBiggestVolumes(tradeActivities).take(1)
    stopWatch.stop()
    stringJoiner.add(s"Sessionizing logic completed in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.toString
  }
}