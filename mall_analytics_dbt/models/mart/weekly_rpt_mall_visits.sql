WITH weekly_visits AS (
    SELECT
        d.weeknumber AS Visit_Week,
        j.visitor_id AS Visitor,
        COUNT(DISTINCT d.fulldate) AS Weekly_Visit_Count
    FROM
        {{ ref('fct_journey') }} j
    LEFT JOIN 
        {{ source('dimensional_data', 'dim_date') }} d ON j.date_id = d.dateid
    GROUP BY d.weeknumber, j.visitor_id
),
weekly_stats AS (
    SELECT
        Visit_Week,
        COUNT(*) AS Total_Visitors,
        SUM(CASE WHEN Weekly_Visit_Count > 1 THEN 1 ELSE 0 END) AS Repeat_Visitors
    FROM 
        weekly_visits
    GROUP BY Visit_Week
)
SELECT 
    Visit_Week,
    Repeat_Visitors,
    (Repeat_Visitors * 1.0 / Total_Visitors) AS Repetition_Rate
FROM 
    weekly_stats
ORDER BY Visit_Week
