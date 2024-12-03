select
    id as source_id,
    name || '_' || cast(source_id as string) as surrogate_key,
    name as zone_name,
    case when has_adverts = 1 then 'Yes' else 'No' end as Has_Adverts,
    case when has_entrance = 1 then 'Yes' else 'No' end as Has_Entrance,
    CURRENT_DATE() as date_created,
    TRUE as is_current
from 
    {{ source('source_data', 'zone')}}