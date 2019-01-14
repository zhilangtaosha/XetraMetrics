package com.acorns.techtest.biggestwinner

import java.lang.Math.max
import java.text.DecimalFormat
import java.time.LocalTime

import com.acorns.techtest.biggestwinner.schema._
import com.acorns.techtest.schema.{DailySecurity, SecurityKey, TradeActivity}
import org.apache.spark.sql.{Dataset, SparkSession}

class DailyBiggestWinners(sparkSession: SparkSession) {
  import sparkSession.implicits._

  def getDailyBiggestWinnersBySessionizing(tradeActivities: Dataset[TradeActivity]): Dataset[DailyBiggestWinner] = {
    tradeActivities
      .groupByKey(_.Date)
      .mapGroups { (date, tradeActivityIterator) =>
        var securitiesMap = Map[SecurityKey, DailySecuritySession]()

        tradeActivityIterator.foreach { tradeActivity =>
          val securityId = tradeActivity.SecurityID
          val description = tradeActivity.SecurityDesc

          val time = tradeActivity.Time
          val startPrice = tradeActivity.StartPrice
          val endPrice = tradeActivity.EndPrice

          val dailySecuritySession = securitiesMap.getOrElse(
            SecurityKey(securityId, description),
            DailySecuritySession(
              date,
              securityId,
              time,
              time,
              startPrice,
              endPrice
            )
          )

          val updatedDailySecuritySession =
            if (LocalTime.parse(time).isBefore(LocalTime.parse(dailySecuritySession.MinTime))
            ) {
              dailySecuritySession.copy(
                MinTime = time,
                StartPrice = tradeActivity.StartPrice
              )
            }
            else if (LocalTime.parse(time).isAfter(LocalTime.parse(dailySecuritySession.MaxTime))
            ) {
              dailySecuritySession.copy(
                MaxTime = time,
                EndPrice = tradeActivity.EndPrice
              )
            }
            else {
              dailySecuritySession
            }

          val updatedSecurityMapping = {
            SecurityKey(securityId, description) -> updatedDailySecuritySession
          }

          securitiesMap += updatedSecurityMapping
        }

        securitiesMap.toArray
          .map { case (securityKey, dailySecuritySession) =>
            val df = new DecimalFormat("#.####")
            val double = (dailySecuritySession.EndPrice - dailySecuritySession.StartPrice) / dailySecuritySession.StartPrice

            DailyBiggestWinner(
              new StringBuilder().append(date).append(".").append(securityKey.SecurityID).toString(),
              date,
              securityKey.SecurityID,
              securityKey.Description,
              df.format(double).toDouble
            )
          }
          .maxBy(_.PercentChange)
      }
      .sort("Date")
  }

  def getDailyBiggestWinnersByJoining(tradeActivities: Dataset[TradeActivity]): Dataset[DailyBiggestWinner] = {
    val dailySecurityActivityAggs = getDailySecurityActivityAggs(tradeActivities)
    val joinedDailySecurityActivityAggsAndTradeActivities =
      getJoinedDailySecurityActivityAggsAndTradeActivities(tradeActivities, dailySecurityActivityAggs)
    joinedDailySecurityActivityAggsAndTradeActivities.cache()
    val maxReturns = getMaxReturns(joinedDailySecurityActivityAggsAndTradeActivities)
    val allReturns = getAllReturns(joinedDailySecurityActivityAggsAndTradeActivities)
    joinedDailySecurityActivityAggsAndTradeActivities.unpersist()

    maxReturns
      .joinWith(
        allReturns,
        maxReturns("PercentChange") === allReturns("PercentChange") &&
          maxReturns("Date") === allReturns("Date"),
        "left_outer"
      )
      .map{case (maxReturn, allReturn) =>
        val df = new DecimalFormat("#.####")

        DailyBiggestWinner(
          new StringBuilder().append(allReturn.Date).append(".").append(allReturn.SecurityID).toString(),
          allReturn.Date,
          allReturn.SecurityID,
          allReturn.Description,
          df.format(allReturn.PercentChange).toDouble
        )
      }
      .sort("Date")
  }

  private def getAllReturns(joinedTradeActivitiesAndDailySecurityAggs: Dataset[((DailySecurityActivityAgg, TradeActivity), TradeActivity)]):
  Dataset[SecurityReturn] = {

    joinedTradeActivitiesAndDailySecurityAggs
      .map{case ((dailySecurityAgg, tradeActivity1), tradeActivity2) =>
        SecurityReturn(
          tradeActivity1.SecurityID,
          tradeActivity1.SecurityDesc,
          tradeActivity1.Date,
          (tradeActivity2.EndPrice - tradeActivity1.StartPrice) / tradeActivity1.StartPrice
        )
      }
  }

  private def getMaxReturns(joinedTradeActivitiesAndDailySecurityAggs: Dataset[((DailySecurityActivityAgg, TradeActivity), TradeActivity)]):
  Dataset[DailyMaxReturn] = {

    joinedTradeActivitiesAndDailySecurityAggs
      .map{case ((dailySecurityAgg, tradeActivity1), tradeActivity2) =>
        DailyMaxReturn(
          dailySecurityAgg.Date,
          (tradeActivity2.EndPrice - tradeActivity1.StartPrice) / tradeActivity1.StartPrice
        )
      }
      .groupByKey(_.Date)
      .mapGroups{(date, maxReturnIterator) =>
        var percentChange = Double.MinValue
        val maxReturn = DailyMaxReturn(
          date,
          percentChange
        )

        maxReturnIterator.foreach{maxReturn =>
          percentChange = max(maxReturn.PercentChange, percentChange)
        }

        maxReturn.copy(PercentChange = percentChange)
      }
  }

  private def getJoinedDailySecurityActivityAggsAndTradeActivities(tradeActivities: Dataset[TradeActivity],
                                                           dailySecurityAggs: Dataset[DailySecurityActivityAgg]):
  Dataset[((DailySecurityActivityAgg, TradeActivity), TradeActivity)] = {

    dailySecurityAggs
      .joinWith(
        tradeActivities,
        dailySecurityAggs("SecurityID") === tradeActivities("SecurityID") &&
          dailySecurityAggs("Date") === tradeActivities("Date") &&
          dailySecurityAggs("MinTime") === tradeActivities("Time"),
        "left_outer"
      )
      .as("join1")
      .joinWith(
        tradeActivities,
        $"join1._1.SecurityID" === tradeActivities("SecurityID") &&
          $"join1._1.Date" === tradeActivities("Date") &&
          $"join1._1.MaxTime" === tradeActivities("Time"),
        "left_outer"
      )
  }

  private def getDailySecurityActivityAggs(tradeActivities: Dataset[TradeActivity]): Dataset[DailySecurityActivityAgg] = {
    tradeActivities
      .groupByKey { tradeActivity =>
        DailySecurity(
          tradeActivity.Date,
          tradeActivity.SecurityID,
          tradeActivity.SecurityDesc
        )
      }
      .mapGroups { (key, tradeActivityIterator) =>
        var minTime = LocalTime.MAX
        var maxTime = LocalTime.MIN
        val dailySecurityAgg = DailySecurityActivityAgg(
          key.Date,
          key.SecurityID,
          minTime.toString,
          maxTime.toString
        )

        tradeActivityIterator.foreach { tradeActivity =>
          val localTime = LocalTime.parse(tradeActivity.Time)
          if (localTime.isBefore(minTime)) {
            minTime = localTime
          }
          if (localTime.isAfter(maxTime)) {
            maxTime = localTime
          }
        }

        dailySecurityAgg.copy(
          MinTime = minTime.toString,
          MaxTime = maxTime.toString
        )
      }
  }
}