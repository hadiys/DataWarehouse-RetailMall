select 
    shop.id as source_id,
    dim_zone.surrogate_key as zone_key,
    cast(shop.id as string) || '_' || cast(shop.zone_id as string) as surrogate_key,
    shop.name as shop_name,
    shop.department,
    CURRENT_DATE() as date_created,
    TRUE as is_current
from
    {{ source('source_data', 'shop') }}
left join
    {{ ref('dim_zone') }} 
        on shop.zone_id = dim_zone.source_id
