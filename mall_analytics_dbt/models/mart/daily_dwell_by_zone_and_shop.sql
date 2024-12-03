with labelled_rows as (
    select 
        d.fulldate as Date,
        t0.fulltime as Time,
        z.zone_name as Zone,
        s.shop_name as Shop,
        t1.fulltime as Zone_Entry_Time, 
        t2.fulltime as Zone_Exit_Time,
        t3.fulltime as Shop_Entry_Time, 
        t4.fulltime as Shop_Exit_Time
    from 
    {{ ref('int_journey_dwell_time') }} j 
    left join 
        {{ source('dimensional_data', 'dim_date') }} d on j.date_id = d.dateid 
    left join 
        {{ source('dimensional_data', 'dim_time') }} t0 on j.timestamp_id = t0.timeid
    left join 
        {{ source('dimensional_data', 'dim_time') }} t1 on j.zone_entry_time_id = t1.timeid
    left join 
        {{ source('dimensional_data', 'dim_time') }} t2 on j.zone_exit_time_id = t2.timeid
    left join 
        {{ source('dimensional_data', 'dim_time') }} t3 on j.shop_entry_time_id = t3.timeid
    left join 
        {{ source('dimensional_data', 'dim_time') }} t4 on j.shop_exit_time_id = t4.timeid
    left join 
        {{ ref('dim_zone') }} z  on j.zone_key = z.surrogate_key
    left outer join 
        {{ ref('dim_shop') }} s on j.shop_key = s.surrogate_key
    order by d.fulldate, t0.fulltime, z.zone_name
)
select
    coalesce(TO_VARCHAR(Date), 'All Time') as Date,
    coalesce(Zone, 'All Zones') as Zone,
    coalesce(Shop, 
        case 
            when count(*) - count(Shop) = 0 then 'All Shops'
            when count(Shop) > 0 then 'All Shops & Zone' 
            else 'Zone Only' 
        end
    ) as Shop,
    sum(case 
            when Shop_Entry_Time is not null and Shop_Exit_Time is not null then timediff(minute, Shop_Entry_Time, Shop_Exit_Time)
            when Zone_Entry_Time is not null and Zone_Exit_Time is not null and Shop_Entry_Time is null then timediff(minute, Zone_Entry_Time, Zone_Exit_Time)
            else 0
        end
    ) as Dwell_Time
from 
    labelled_rows 
group by rollup (
    Date,
    Zone,
    Shop
)
order by Date, Zone, Shop
