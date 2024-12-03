select 
    transaction.id, 
    dim_shop.surrogate_key as shop_key,
    visitor_id,
    transaction.amount,
    transaction.payment_type,
    transaction.hashed_creditcard,
    dim_date.dateid as date_id,
    dim_time.timeid as time_id
from
    {{ source('source_data', 'transaction') }}
left join
    {{ ref('dim_shop') }} on transaction.shop_id = dim_shop.source_id
left join
    {{ source('dimensional_data', 'dim_date') }} on DATE(transaction.datetime) = dim_date.fulldate
left join 
    {{ source('dimensional_data', 'dim_time') }} on TO_CHAR(transaction.datetime, 'HH24:MI:SS') = dim_time.fulltime

