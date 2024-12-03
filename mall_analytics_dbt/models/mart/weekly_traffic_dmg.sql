SELECT 
    d.weeknumber AS Week,
    v.gender AS Gender,
    v.age_group AS Age_Group,
    v.income_level AS Income_Level,
    COUNT(DISTINCT j.visitor_id) AS Unique_Visits
FROM 
    {{ ref('fct_journey') }} j
LEFT JOIN 
    {{ ref('dim_visitor') }} v ON j.visitor_id = v.id
LEFT JOIN
    {{ source('dimensional_data', 'dim_date') }} d ON d.dateid = j.date_id
GROUP BY d.weeknumber, v.gender, v.age_group, v.income_level
ORDER BY d.weeknumber