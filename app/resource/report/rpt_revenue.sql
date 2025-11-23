WITH 


revenue AS (


    SELECT

        monthly_revenue_key  AS revenue_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        revenue_type,
        month_roll,
        arr,
        mrr,
        customer_join_month   AS customer_cohort,
        DATEDIFF(MONTH,customer_join_month, month_roll) AS customer_tenure,
        CASE 
            WHEN month_roll <= customer_churn_month 
                THEN SUM(mrr) OVER (
                    PARTITION BY r.customer_level_1
                    ORDER BY month_roll)
            ELSE 0
        END  AS cltv,
        volume,
        CASE
            WHEN month_roll = customer_join_month 
            THEN 1 
            ELSE 0 
        END AS is_customer_new,
        CASE
            WHEN month_roll = customer_churn_month 
            THEN 1 
            ELSE 0 
        END AS is_customer_churn

    FROM {{ ref('monthly_revenue') }} AS r

    LEFT JOIN 
        {{ ref('customer_contract') }} AS c
    ON r.customer_level_1 = c.customer_level_1
        -- AND r.customer_level_2 = c.customer_level_2
)

SELECT * FROM revenue