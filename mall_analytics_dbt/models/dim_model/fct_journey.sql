select  
    j.id,
    s.surrogate_key as shop_key,
    z.surrogate_key as zone_key,
    j.visitor_id,
    d.dateid as date_id,
    t.timeid as time_id
from    
    {{ source('source_data', 'journey') }} j
left join
    {{ ref('dim_shop') }} s on j.shop_id = s.source_id
left join
    {{ ref('dim_zone') }} z on j.zone_id = z.source_id
left join
    {{ source('dimensional_data', 'dim_date') }} d on DATE(j.timestamp) = d.fulldate
left join 
    {{ source('dimensional_data', 'dim_time') }} t on TO_CHAR(j.timestamp, 'HH24:MI:SS') = t.fulltime  
