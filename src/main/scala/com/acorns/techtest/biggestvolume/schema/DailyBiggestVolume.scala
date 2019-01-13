package com.acorns.techtest.biggestvolume.schema

case class DailyBiggestVolume(Date: String,
                              SecurityID: Int,
                              Description: String,
                              MaxAmount: Double)