select 
    id,
    gender,
    age, 
    case 
        when age between 18 and 25 then '18-25' 
        when age between 26 and 29 then '26-29' 
        when age between 30 and 39 then '30-39' 
        when age between 40 and 49 then '40-49' 
        when age between 50 and 59 then '50-59' 
        when age > 60 then '60-100' 
        else 'undefined_age_group' 
    end as age_group,
    income_level as income,
    case    
        when income_level between 20000 and 34999 then '20-35K'
        when income_level between 35000 and 54999 then '35-55K'
        when income_level between 55000 and 74999 then '55-75K'
        when income_level between 75000 and 89999 then '75-90K'
        when income_level between 90000 and 149999 then '90-150K'
        when income_level >= 150000 then '150K+'
        else 'undefined_income_level'
    end as income_level,
    device_id
from 
    {{ source('source_data', 'visitor') }}
