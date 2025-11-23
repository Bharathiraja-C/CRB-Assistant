WITH
    
get_product_start_end_month AS (
    
    SELECT

        customer_level_1,
        -- customer_level_2,
        product_level_1,
        -- product_level_2,
        MIN(month_roll) OVER (PARTITION BY customer_level_1, product_level_1)                          AS product_start_month,
        MAX(month_roll) OVER (PARTITION BY customer_level_1, product_level_1)                          AS product_end_month,
        DATEADD(MONTH, 1, MAX(month_roll) OVER (PARTITION BY customer_level_1, product_level_1))       AS product_churn_month

    FROM
        {{ ref('monthly_revenue') }}
    WHERE mrr <> 0

),

customer_product_contract AS (

SELECT

    customer_level_1,
    -- customer_level_2,
    product_level_1,
    -- product_level_2,
    product_start_month,
    product_end_month,
    product_churn_month

FROM get_product_start_end_month

GROUP BY
    customer_level_1,
    -- customer_level_2,
    product_level_1,
    -- product_level_2,
    product_start_month,
    product_end_month,
    product_churn_month

)

SELECT * FROM customer_product_contract