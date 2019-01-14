package com.acorns.techtest.biggestvolume.schema

case class DailyBiggestVolume(uniqueIdentifier: String,
                              Date: String,
                              SecurityID: Int,
                              Description: String,
                              MaxAmount: Double)