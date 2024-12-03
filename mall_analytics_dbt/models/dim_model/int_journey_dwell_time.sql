select 
    tp.id,
    tp.zone_key,
    tp.shop_key,
    tp.visitor_id,
    d.dateid as date_id,
    t1.timeid as timestamp_id,
    t2.timeid as zone_entry_time_id,
    t3.timeid as zone_exit_time_id,
    t4.timeid as shop_entry_time_id,
    t5.timeid as shop_exit_time_id
from
    {{ source('dimensional_data', 'temp') }} as tp
left join
    {{ source('dimensional_data', 'dim_date') }} as d on tp.date = d.fulldate
left join
    {{ source('dimensional_data', 'dim_time') }} as t1 on TO_CHAR(tp.timestamp, 'HH24:MI:SS') = t1.fulltime
left join
    {{ source('dimensional_data', 'dim_time') }} as t2 on TO_CHAR(tp.zone_entry, 'HH24:MI:SS') = t2.fulltime
left join
    {{ source('dimensional_data', 'dim_time') }} as t3 on TO_CHAR(tp.zone_exit, 'HH24:MI:SS') = t3.fulltime
left join
    {{ source('dimensional_data', 'dim_time') }} as t4 on TO_CHAR(tp.shop_entry, 'HH24:MI:SS') = t4.fulltime
left join
    {{ source('dimensional_data', 'dim_time') }} as t5 on TO_CHAR(tp.shop_exit, 'HH24:MI:SS') = t5.fulltime
