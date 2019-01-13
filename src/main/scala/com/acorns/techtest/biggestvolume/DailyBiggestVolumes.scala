package com.acorns.techtest.biggestvolume

import java.lang.Math.max

import com.acorns.techtest.biggestvolume.schema.{DailyBiggestVolume, DailyMaxAmount, DailySecurityVolumeAgg}
import com.acorns.techtest.schema.{DailySecurity, SecurityKey, TradeActivity}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Dataset, SparkSession}

class DailyBiggestVolumes(sparkSession: SparkSession) {
  import sparkSession.implicits._

  def getDailyBiggestVolumesBySessionizing(tradeActivities: Dataset[TradeActivity]): Dataset[DailyBiggestVolume] = {
    tradeActivities
      .groupByKey(_.Date)
      .mapGroups { (date, tradeActivityIterator) =>
        var securitiesMap = Map[SecurityKey, DailySecurityVolumeAgg]()

        tradeActivityIterator.foreach{tradeActivity =>
          val securityId = tradeActivity.SecurityID
          val description = tradeActivity.SecurityDesc

          val dailySecurityVolumeAgg = securitiesMap.getOrElse(
            SecurityKey(securityId, description),
            DailySecurityVolumeAgg(
              date,
              securityId,
              description,
              0.0
            )
          )

          val updatedDailySecurityVolumeAgg = dailySecurityVolumeAgg.copy(
            TradeAmountMM = dailySecurityVolumeAgg.TradeAmountMM +
              (tradeActivity.StartPrice * tradeActivity.TradedVolume)
          )

          val updatedSecurityMapping = {
            SecurityKey(securityId, description) -> updatedDailySecurityVolumeAgg
          }

          securitiesMap += updatedSecurityMapping
        }

        securitiesMap.toArray
          .map{case (securityKey, dailySecurityVolumeAgg) =>
            DailyBiggestVolume(
              date,
              securityKey.SecurityID,
              securityKey.Description,
              dailySecurityVolumeAgg.TradeAmountMM / "1e6".toDouble
            )
          }
          .maxBy(_.MaxAmount)
      }
      .sort($"Date", $"MaxAmount".desc)
  }

  def getDailyBiggestVolumesByJoining(tradeActivities: Dataset[TradeActivity]): Dataset[DailyBiggestVolume] = {
    val dailySecurityVolumeAggs = getDailySecurityVolumeAggs(tradeActivities)
    dailySecurityVolumeAggs.cache()
    val dailyMaxAmounts = getDailyMaxAmounts(dailySecurityVolumeAggs)

    getDailyBiggestVolumes(dailyMaxAmounts, dailySecurityVolumeAggs)
  }

  private def getDailyBiggestVolumes(dailyMaxAmounts: Dataset[DailyMaxAmount],
                                     dailySecurityVolumeAggs: Dataset[DailySecurityVolumeAgg]): Dataset[DailyBiggestVolume] = {

    dailyMaxAmounts
      .joinWith(
        dailySecurityVolumeAggs,
        dailyMaxAmounts("MaxAmount") === dailySecurityVolumeAggs("TradeAmountMM") &&
          dailyMaxAmounts("Date") === dailySecurityVolumeAggs("Date"),
        "inner"
      )
      .map{case (dailyMaxAmount, dailySecurityVolumeAgg) =>
        DailyBiggestVolume(
          dailyMaxAmount.Date,
          dailySecurityVolumeAgg.SecurityID,
          dailySecurityVolumeAgg.Description,
          dailyMaxAmount.MaxAmount
        )
      }
      .sort($"Date", desc("MaxAmount"))
  }

  private def getDailyMaxAmounts(dailySecurityVolumeAggs: Dataset[DailySecurityVolumeAgg]): Dataset[DailyMaxAmount] = {
    dailySecurityVolumeAggs
      .groupByKey(_.Date)
      .mapGroups { (date, dailySecurityVolumeAggIterator) =>
        var maxAmount = Double.MinValue

        dailySecurityVolumeAggIterator.foreach{dailySecurityVolumeAgg =>
          maxAmount = max(dailySecurityVolumeAgg.TradeAmountMM, maxAmount)
        }

        DailyMaxAmount(
          date,
          maxAmount
        )
      }
  }

  private def getDailySecurityVolumeAggs(tradeActivities: Dataset[TradeActivity]): Dataset[DailySecurityVolumeAgg] = {
    tradeActivities
      .groupByKey { tradeActivity =>
        DailySecurity(
          tradeActivity.Date,
          tradeActivity.SecurityID,
          tradeActivity.SecurityDesc
        )
      }
      .mapGroups { (key, tradeActivityIterator) =>
        var tradeAmountMM = 0.0

        tradeActivityIterator.foreach { tradeActivity =>
          tradeAmountMM += (tradeActivity.StartPrice * tradeActivity.TradedVolume)
        }

        DailySecurityVolumeAgg(
          key.Date,
          key.SecurityID,
          key.Description,
          tradeAmountMM / "1e6".toDouble
        )
      }
  }
}