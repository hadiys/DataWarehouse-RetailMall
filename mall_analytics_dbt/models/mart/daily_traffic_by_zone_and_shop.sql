WITH unique_visits AS (
    SELECT DISTINCT
        d.fulldate AS Visit_Date,
        z.zone_name AS Zone,
        s.department AS Department,
        s.shop_name AS Shop,
        j.visitor_id AS Visitor
    FROM
        {{ ref('fct_journey') }} as j
    LEFT OUTER JOIN 
        {{ ref('dim_shop') }} s ON j.shop_key = s.surrogate_key
    LEFT JOIN 
        {{ ref('dim_zone') }} z ON j.zone_key = z.surrogate_key
    LEFT JOIN 
        {{ source('dimensional_data', 'dim_date') }} d ON j.date_id = d.dateid
) 
SELECT 
    COALESCE(TO_VARCHAR(Visit_Date),'All Time')  AS Visit_Date,
    COALESCE(Zone, 'All Zones') AS Zone,
    COALESCE(Department, 
        CASE 
                WHEN COUNT(*) - COUNT(Shop) = 0 THEN 'All Departments'
                WHEN COUNT(Shop) > 0 THEN 'All Departments & Zone' 
                ELSE 'Zone Only' 
        END
    ) AS Department,
    COALESCE(Shop, 
        CASE 
                WHEN COUNT(*) - COUNT(Shop) = 0 THEN 'All Shops'
                WHEN COUNT(Shop) > 0 THEN 'All Shops & Zone' 
                ELSE 'Zone Only' 
        END 
    ) AS Shop,
    COUNT(DISTINCT Visitor) AS Unique_Visits
FROM 
    unique_visits
GROUP BY ROLLUP (
    Visit_Date,
    Zone,
    Department,
    Shop
)
ORDER BY Visit_Date, Zone, Department, Shop