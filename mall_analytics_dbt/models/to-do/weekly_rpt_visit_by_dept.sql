WITH unique_dept_visits AS (
    SELECT DISTINCT
        d.fulldate AS Visit_Date,
        -- z.zone_name AS Zone,
        s.department AS Department,
        -- s.shop_name AS Shop,
        j.visitor_id AS Visitor
    FROM
        fct_journey as j
    LEFT OUTER JOIN 
        dim_shop s ON j.shop_key = s.surrogate_key
    -- LEFT JOIN 
    --     dim_zone z ON j.zone_key = z.surrogate_key
    LEFT JOIN 
        dim_date d ON j.date_id = d.dateid
WHERE 
d.fulldate BETWEEN '2024-01-01' AND '2024-01-31'
AND s.department is not null
) 
SELECT * FROM unique_dept_visits;