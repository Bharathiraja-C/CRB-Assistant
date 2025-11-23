WITH

churn_month AS (

    SELECT

        customer_level_1,
        -- customer_level_2,
        MIN(month_roll)                      AS customer_join_month,
        MAX(month_roll)                      AS customer_end_month,
        DATEADD(MONTH, 1, MAX(month_roll))   AS customer_churn_month 

    FROM {{ ref('monthly_revenue') }}
    WHERE
        mrr <> 0.00

    GROUP BY 
        customer_level_1
        -- ,customer_level_2

),

customer_contract AS (

SELECT 

    customer_level_1,
    -- customer_level_2,
    customer_join_month,
    customer_end_month,
    customer_churn_month

FROM churn_month 

) 

SELECT * FROM customer_contract
