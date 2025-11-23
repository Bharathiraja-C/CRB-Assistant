WITH
    
date_joins AS (

    SELECT
    -- Joining Customer and Revenue Data  
        r.*,
        c.customer_level_1,
        -- c.customer_level_2,
        p.product_level_1,
        -- p.product_level_2,
        r.revenue_type,
        MIN(month) OVER (PARTITION BY revenue_key,r.revenue_type) AS segment_start_month,
        MAX(month) OVER (PARTITION BY revenue_key,r.revenue_type) AS segment_end_month

    FROM {{ ref('fact_revenue') }} AS r

    LEFT JOIN 
        {{ ref('dim_customer') }} AS c
    ON r.customer_key = c.customer_key

    LEFT JOIN 
        {{ ref('dim_product') }} AS p 
    ON r.product_key = p.product_key
    
    WHERE 
        r.revenue <> 0.00

),

date_scaffolding AS (

    SELECT 

        revenue_key,
        customer_key,
        entity_key,
        customer_level_1,
        -- customer_level_2,
        product_key,
        product_level_1,
        -- product_level_2,
        other_key,
        c.month_roll,
        CASE
            WHEN c.month_roll > d.month 
            OR c.month_roll <> d.month 
                THEN 0 
            ELSE  d.revenue
        END AS revenue,
        CASE
            WHEN c.month_roll > d.month 
            OR c.month_roll <> d.month 
                THEN 0 
            ELSE  d.volume
        END AS volume,
        revenue_type

    FROM
        {{ ref('dim_calendar') }} AS c

    JOIN 
        date_joins AS d
    ON c.month_roll <= DATEADD(MONTH,12, d.segment_end_month) 
    AND c.month_roll >= d.segment_start_month

),

aggrageted_revenue AS (

    SELECT 
        revenue_key                             AS monthly_revenue_key,
        customer_key,
        entity_key,
        customer_level_1,
        -- customer_level_2,
        product_key,
        product_level_1,
        -- product_level_2,
        other_key,
        month_roll,
        revenue_type,
        SUM(revenue)                                            AS mrr,
        SUM(volume)                                            AS volume,
        MONTH(DATEADD(MONTH,-4+1, month_roll))   AS ytd_helper    -- Add 1 back to YTD year start here so YTD start aligns with month selected i.e. 4 = start in April

    FROM
        date_scaffolding
    GROUP BY
        revenue_key,
        customer_key,
        entity_key,
        customer_level_1,
        -- customer_level_2,
        product_key,
        product_level_1,
        -- product_level_2,
        other_key,
        revenue_type,
        month_roll

),

arr_calc AS (

    SELECT

        monthly_revenue_key,
        customer_key,
        entity_key,
        customer_level_1,
        -- customer_level_2,
        product_key,
        product_level_1,
        -- product_level_2,
        other_key,
        m.revenue_type AS revenue_type,
        month_roll,
        CASE 
            WHEN m.revenue_type = 1 OR m.revenue_type = 'Recurring' THEN mrr * 12
            ELSE SUM(mrr) OVER (
                    PARTITION BY monthly_revenue_key
                    ORDER BY month_roll 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
        END AS arr,
        mrr,
        volume,
        ytd_helper

    FROM aggrageted_revenue AS m

    LEFT JOIN 
        churn_month_calc AS c 
    ON c.customer = m.customer_level_1
        AND c.product = m.product_level_1

) 

SELECT * FROM arr_calc