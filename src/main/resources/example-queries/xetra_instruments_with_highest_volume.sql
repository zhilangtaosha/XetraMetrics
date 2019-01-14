select
    cast(SecurityID as string) as uniqueIdentifier,
    SecurityID,
    SecurityDesc as Description,
    round(avg((MaxPrice - MinPrice) / MinPrice), 4) as ImpliedVolume
from xetra
group by SecurityDesc, SecurityID
order by ImpliedVolume desc