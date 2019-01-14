select
    concat(allreturns.Date, '.', allreturns.SecurityID) as uniqueIdentifier,
    allreturns.Date,
    allreturns.SecurityID,
    allreturns.Description,
    round(allreturns.PercentChange, 4) as PercentChange

from (

    select xetra1.Date, max((xetra2.EndPrice - xetra1.StartPrice) / xetra1.StartPrice) as PercentChange

    from (

        select SecurityID, min(Time) as MinTime, max(Time) as MaxTime, Date from xetra
        group by SecurityID, Date
    ) as timestamps

    left join xetra as xetra1
    on xetra1.SecurityID = timestamps.SecurityID and xetra1.Date = timestamps.Date and xetra1.Time = timestamps.MinTime

    left join xetra as xetra2
    on xetra2.SecurityID = timestamps.SecurityID and xetra2.Date = timestamps.Date and xetra2.Time = timestamps.MaxTime

    group by xetra1.Date
) as maxreturns

left join (
    select xetra1.SecurityID, xetra1.securitydesc as Description, xetra1.Date, (xetra2.EndPrice - xetra1.StartPrice) / xetra1.StartPrice as PercentChange

    from (

        select SecurityID, min(Time) as MinTime, max(Time) as MaxTime, Date from xetra
        group by SecurityID, Date
    ) as timestamps

    left join xetra as xetra1
    on xetra1.SecurityID = timestamps.SecurityID and xetra1.Date = timestamps.Date and xetra1.Time = timestamps.MinTime

    left join xetra as xetra2
    on xetra2.SecurityID = timestamps.SecurityID and xetra2.Date = timestamps.Date and xetra2.Time = timestamps.MaxTime
) as allreturns

on maxreturns.PercentChange = allreturns.PercentChange and maxreturns.Date = allreturns.Date

order by allreturns.Date