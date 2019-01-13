package com.acorns.techtest.biggestvolume.schema

case class DailySecurityVolumeAgg(Date: String,
                                  SecurityID: Int,
                                  Description: String,
                                  TradeAmountMM: Double)