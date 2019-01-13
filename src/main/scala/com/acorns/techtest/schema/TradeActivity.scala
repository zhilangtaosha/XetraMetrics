package com.acorns.techtest.schema

case class TradeActivity(ISIN: String,
                         Mnemonic: String,
                         SecurityDesc: String,
                         SecurityType: String,
                         Currency: String,
                         SecurityID: Int,
                         Date: String,
                         Time: String,
                         StartPrice: Double,
                         MaxPrice: Double,
                         MinPrice: Double,
                         EndPrice: Double,
                         TradedVolume: Int,
                         NumberOfTrades: Int)