select 
    coalesce(to_varchar(d.fulldate), 'Grand Total') as Date,
    coalesce(s.department, 'All Departments') as Department,
    coalesce(s.shop_name, 'All Shops') as Shop,
    sum(f.amount) as Transaction_Amount,
from 
    {{ ref('fct_transaction') }} f
left join
    {{ ref('dim_shop') }} s on s.surrogate_key = f.shop_key
left join
    {{ source('dimensional_data', 'dim_date') }} d on d.dateid = f.date_id
group by rollup (
    d.fulldate,
    s.department,
    s.shop_name
)
order by d.fulldate, s.department, s.shop_name