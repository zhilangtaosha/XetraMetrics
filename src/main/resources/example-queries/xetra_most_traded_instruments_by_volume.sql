select
    concat(sumtable.Date, '.', sumtable.SecurityID) as uniqueIdentifier,
    sumtable.Date,
    sumtable.SecurityID,
    sumtable.Description,
    round(maxtable.MaxAmount, 4) as MaxAmount

from (
    select Date, max(TradeAmountMM) as MaxAmount

    from (

        select SecurityID, SecurityDesc as Description, Date, sum(StartPrice * TradedVolume) / 1e6 as TradeAmountMM
        from xetra
        group by SecurityID, Description, Date
        order by Date asc, TradeAmountMM desc
    )

    group by Date
) as maxtable

left join (
    select SecurityID, SecurityDesc as Description, Date, sum(StartPrice * TradedVolume) / 1e6 as TradeAmountMM
    from xetra
    group by SecurityID, Description, Date
    order by Date asc, TradeAmountMM desc
) as sumtable

on sumtable.TradeAmountMM = maxtable.MaxAmount and sumtable.Date = maxtable.Date

order by sumtable.Date asc, maxtable.MaxAmount desc