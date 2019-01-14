package com.acorns.techtest.securityvolume

import java.text.DecimalFormat

import com.acorns.techtest.schema.{SecurityKey, TradeActivity}
import com.acorns.techtest.securityvolume.schema.BiggestVolume
import org.apache.spark.sql.{Dataset, SparkSession}

class BiggestVolumes(sparkSession: SparkSession) {
  import sparkSession.implicits._

  def getBiggestVolumes(tradeActivities: Dataset[TradeActivity]): Dataset[BiggestVolume] = {
    tradeActivities
      .groupByKey(tradeActivity =>
        SecurityKey(
          tradeActivity.SecurityID,
          tradeActivity.SecurityDesc
        )
      )
      .flatMapGroups { (securityKey, tradeActivityIterator) =>
        var securitiesMap = Map[SecurityKey, (BiggestVolume, Int)]()

        tradeActivityIterator.foreach { tradeActivity =>
          val securityId = tradeActivity.SecurityID
          val description = tradeActivity.SecurityDesc

          val (biggestVolume, count) = securitiesMap.getOrElse(
            SecurityKey(securityId, description),
            (
              BiggestVolume(
                securityId.toString,
                securityId,
                description,
                0.0
              ),
              0
            )
          )

          val updatedBiggestVolume = biggestVolume.copy(
            ImpliedVolume = biggestVolume.ImpliedVolume + (tradeActivity.MaxPrice - tradeActivity.MinPrice) / tradeActivity.MinPrice
          )
          val updatedCount = count + 1

          val updatedSecurityMapping = {
            securityKey -> (updatedBiggestVolume, updatedCount)
          }

          securitiesMap += updatedSecurityMapping
        }

        securitiesMap.toArray
          .map{case (securityKey, (biggestVolume, count)) =>
            val df = new DecimalFormat("#.####")
            val double = biggestVolume.ImpliedVolume / count

            biggestVolume.copy(
              ImpliedVolume = df.format(double).toDouble
            )
          }
      }
      .sort($"ImpliedVolume".desc)
  }
}
