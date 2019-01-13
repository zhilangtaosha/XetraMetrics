package com.acorns.techtest.biggestwinner.schema

case class DailySecuritySession(Date: String,
                                SecurityID: Int,
                                MinTime: String,
                                MaxTime: String,
                                StartPrice: Double,
                                EndPrice: Double)