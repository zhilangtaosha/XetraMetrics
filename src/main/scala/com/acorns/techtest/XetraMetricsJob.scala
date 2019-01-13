package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.biggestvolume.DailyBiggestVolumes
import com.acorns.techtest.biggestwinner.DailyBiggestWinners
import com.acorns.techtest.schema.TradeActivity
import com.acorns.techtest.securityvolume.SecurityVolumes
import com.acorns.techtest.util.OutputStringUtils
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.functions.concat
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

    stringJoiner.add(OutputStringUtils.getSampleTitle("source"))
    tradeActivities.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")
    stringJoiner.add("")

    stringJoiner
      .add(s"/${OutputStringUtils.repeatChar(29, '*')}")
      .add(s"${OutputStringUtils.repeatChar(5, ' ')}DAILY BIGGEST WINNERS")
      .add(s"${OutputStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestWinnersSample(tradeActivities))
      .add("")
      .add("")
      .add(s"/${OutputStringUtils.repeatChar(29, '*')}")
      .add(s"${OutputStringUtils.repeatChar(5, ' ')}DAILY BIGGEST VOLUMES")
      .add(s"${OutputStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getDailyBiggestVolumesSample(tradeActivities))
      .add("")
      .add("")
      .add(s"/${OutputStringUtils.repeatChar(29, '*')}")
      .add(s"${OutputStringUtils.repeatChar(5, ' ')}MOST TRADED STOCK/ETF")
      .add(s"${OutputStringUtils.repeatChar(29, '*')}/")
      .add("")
      .add(getBiggestVolumesSample(tradeActivities))
      .add("")
      .add("")

    stringJoiner.add(
      s"This application completed in ${stopWatch.getTime} ms."
    )

    println(stringJoiner.toString)
  }

  private def getDailyBiggestWinnersSample(tradeActivities: Dataset[TradeActivity]): String = {
    val stopWatch = new StopWatch

    val label1 = "sessionizing"
    val label2 = "joining"

    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestWinners = new DailyBiggestWinners(sparkSession)

    val dailyBiggestWinnersBySessionizing = dailyBiggestWinners.getDailyBiggestWinnersBySessionizing(tradeActivities)
    val dailyBiggestWinnersByJoining = dailyBiggestWinners.getDailyBiggestWinnersByJoining(tradeActivities)

    dailyBiggestWinnersBySessionizing.cache()
    stopWatch.start()
    val sessionizingCount = dailyBiggestWinnersBySessionizing.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $sessionizingCount daily biggest winners by $label1 in ${stopWatch.getTime} ms.")

    stringJoiner.add(OutputStringUtils.getSampleTitle(label1))
    dailyBiggestWinnersBySessionizing.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    stopWatch.reset()
    stopWatch.start()
    dailyBiggestWinnersByJoining.cache()
    val joiningCount = dailyBiggestWinnersByJoining.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $joiningCount daily biggest winners by $label2 in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.add(OutputStringUtils.getSampleTitle(label2))
    dailyBiggestWinnersByJoining.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    val dailyBiggestWinnerComparison = new DataFrameComparison(
      sparkSession,
      dailyBiggestWinnersBySessionizing.withColumn("uniqueIdentifier", concat($"Date", $"SecurityID")),
      dailyBiggestWinnersByJoining.withColumn("uniqueIdentifier", concat($"Date", $"SecurityID"))
    )

    stringJoiner.add(
      dailyBiggestWinnerComparison.getComparisonSamples(label1, label2)
    )

    stringJoiner.toString
  }

  private def getDailyBiggestVolumesSample(tradeActivities: Dataset[TradeActivity]): String = {
    val stopWatch = new StopWatch

    val label1 = "sessionizing"
    val label2 = "joining"

    val stringJoiner = new StringJoiner("\n")

    val dailyBiggestVolumes = new DailyBiggestVolumes(sparkSession)

    val dailyBiggestVolumesBySessionizing = dailyBiggestVolumes.getDailyBiggestVolumesBySessionizing(tradeActivities)
    val dailyBiggestVolumesByJoining = dailyBiggestVolumes.getDailyBiggestVolumesByJoining(tradeActivities)

    dailyBiggestVolumesBySessionizing.cache()
    stopWatch.start()
    val sessionizingCount = dailyBiggestVolumesBySessionizing.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $sessionizingCount daily biggest volumes by $label1 in ${stopWatch.getTime} ms.")

    stringJoiner.add(OutputStringUtils.getSampleTitle(label1))
    dailyBiggestVolumesBySessionizing.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    stopWatch.reset()
    stopWatch.start()
    dailyBiggestVolumesByJoining.cache()
    val joiningCount = dailyBiggestVolumesByJoining.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $joiningCount daily biggest volumes by $label2 in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.add(OutputStringUtils.getSampleTitle(label2))
    dailyBiggestVolumesByJoining.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)
    stringJoiner.add("")

    val dailyBiggestWinnerComparison = new DataFrameComparison(
      sparkSession,
      dailyBiggestVolumesBySessionizing.withColumn("uniqueIdentifier", concat($"Date", $"SecurityID")),
      dailyBiggestVolumesByJoining.withColumn("uniqueIdentifier", concat($"Date", $"SecurityID"))
    )

    stringJoiner.add(
      dailyBiggestWinnerComparison.getComparisonSamples("sessionizing", "joining")
    )

    stringJoiner.toString
  }

  private def getBiggestVolumesSample(tradeActivities: Dataset[TradeActivity]): String = {
    val stopWatch = new StopWatch

    val stringJoiner = new StringJoiner("\n")

    val securityVolumes = new SecurityVolumes(sparkSession).getSecurityVolumes(tradeActivities)

    securityVolumes.cache()
    stopWatch.start()
    val securityVolumesCount = securityVolumes.count()
    stopWatch.stop()
    stringJoiner.add(s"Found $securityVolumesCount security volumes in ${stopWatch.getTime} ms.")
    stopWatch.reset()

    stringJoiner.add(OutputStringUtils.getSampleTitle("single"))
    securityVolumes.limit(10).collect().foreach{ row =>
      stringJoiner.add(row.toString)
    }
    stringJoiner.add(OutputStringUtils.get30Dashes)

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