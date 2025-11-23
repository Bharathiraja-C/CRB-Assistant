WITH
 
arr_join AS (

    SELECT

        a.*,
        m.arr,
        m.volume,
        p.arr_lm,
        p.arr_l3m,
        p.arr_ltm,
        p.arr_ytd

    FROM
        {{ ref('delta_revenue') }} AS a
        
    INNER JOIN
        {{ ref('monthly_revenue') }} AS m
    ON a.delta_revenue_key = m.monthly_revenue_key
        AND a.month_roll = m.month_roll

    INNER JOIN
        {{ ref('period_revenue') }} AS p
    ON a.delta_revenue_key = p.period_revenue_key
        AND a.month_roll = p.month_roll

    WHERE m.revenue_type IN ('Recurring', 'Re-occurring')

)

, lm_prep AS (

    SELECT

        delta_revenue_key               AS snowball_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        revenue_type,
        month_roll,
        'lm'                          AS period_type,
        arr_lm                      AS bop_arr,
        lm_delta_customer_churn     AS customer_churn,
        lm_delta_downgrade          AS product_churn,
        lm_deactivation             AS deactivation,
        lm_Intermittent_churn       AS Intermittent_churn,
        lm_delta_downsell           AS downsell,
        -- uncomment below fields to find the price volume downsell
        -- lm_delta_price_downsell     AS downsell_price,
        -- lm_delta_volume_downsell    AS downsell_volume,
        lm_delta_upsell            AS upsell,
        -- uncomment below fields to find the price volume upsell
        -- lm_delta_price_upsell       AS upsell_price,
        -- lm_delta_volume_upsell      AS upsell_volume,
        lm_delta_cross_sell         AS cross_sell,
        lm_delta_customer_new       AS new_customer,
        lm_reactivation             AS reactivation,
        lm_winback                  AS winback,
        arr                         AS eop_arr,
        volume                      AS volume

    FROM
        arr_join
)

, l3m_prep AS (

    SELECT

        delta_revenue_key               AS snowball_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        revenue_type,
        month_roll,
        'l3m'                         AS period_type,
        arr_l3m                     AS bop_arr,
        l3m_delta_customer_churn    AS customer_churn,
        l3m_delta_downgrade         AS product_churn,
        l3m_deactivation            AS deactivation,
        l3m_Intermittent_churn      AS Intermittent_churn,
        l3m_delta_downsell          AS downsell,
        -- l3m_delta_price_downsell    AS downsell_price,
        -- l3m_delta_volume_downsell   AS downsell_volume,
        l3m_delta_upsell            AS upsell,
        -- l3m_delta_price_upsell      AS upsell_price,
        -- l3m_delta_volume_upsell     AS upsell_volume,
        l3m_delta_cross_sell        AS cross_sell,
        l3m_delta_customer_new      AS new_customer,
        l3m_reactivation            AS reactivation,
        l3m_winback                 AS winback,
        arr                         AS eop_arr,
        volume                      AS volume  

    FROM
        arr_join
)

, ltm_prep AS (

    SELECT

        delta_revenue_key               AS snowball_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        revenue_type,
        month_roll,
        'ltm'                         AS period_type,
        arr_ltm                     AS bop_arr,
        ltm_delta_customer_churn    AS customer_churn,
        ltm_delta_downgrade         AS product_churn,
        ltm_deactivation            AS deactivation,
        ltm_Intermittent_churn      AS Intermittent_churn,
        ltm_delta_downsell          AS downsell,
        -- ltm_delta_price_downsell    AS downsell_price,
        -- ltm_delta_volume_downsell   AS downsell_volume,
        ltm_delta_upsell            AS upsell,
        -- ltm_delta_price_upsell      AS upsell_price,
        -- ltm_delta_volume_upsell     AS upsell_volume,
        ltm_delta_cross_sell        AS cross_sell,
        ltm_delta_customer_new      AS new_customer,
        ltm_reactivation            AS reactivation,
        ltm_winback                 AS winback,
        arr                         AS eop_arr,
        volume                      AS volume  

    FROM
        arr_join
)

, ltm_non_recurring AS (

    SELECT

        revenue_key                 AS snowball_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        revenue_type,
        month_roll,
        'ltm'                        AS period_type,
        arr_ltm                      AS bop_arr,
        0                            AS customer_churn,
        0                            AS product_churn,
        0                            AS deactivation,
        0                            AS Intermittent_churn,
        0                            AS downsell,
        0                            AS upsell,
        cross_sell                   AS cross_sell,
        new_customer                 AS new_customer,
        0                            AS reactivation,
        0                            AS winback,
        arr                          AS eop_arr,
        0                            AS volume  

    FROM
        {{ ref('non_recurring_revenue') }}
)

, ytd_prep AS(

    SELECT

        delta_revenue_key               AS snowball_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        revenue_type,
        month_roll,
        'ytd'                         AS period_type,
        arr_ytd                     AS bop_arr,
        ytd_delta_customer_churn    AS customer_churn,
        ytd_delta_downgrade         AS product_churn,
        ytd_deactivation            AS deactivation,
        ytd_Intermittent_churn      AS Intermittent_churn,
        ytd_delta_downsell          AS downsell,
        -- ytd_delta_price_downsell    AS downsell_price,
        -- ytd_delta_volume_downsell   AS downsell_volume,
        ytd_delta_upsell            AS upsell,
        -- ytd_delta_price_upsell      AS upsell_price,
        -- ytd_delta_volume_upsell     AS upsell_volume,
        ytd_delta_cross_sell        AS cross_sell,
        ytd_delta_customer_new      AS new_customer,
        ytd_reactivation            AS reactivation,
        ytd_winback                 AS winback,
        arr                         AS eop_arr,
        volume                      AS volume 

    FROM
        arr_join
)

, combined_period_type AS (

    SELECT * FROM lm_prep

    UNION ALL

    SELECT * FROM l3m_prep

    
    UNION ALL
    
    SELECT * FROM ltm_prep

    UNION ALL

    SELECT * FROM ltm_non_recurring
    
    UNION ALL
    
    SELECT * FROM ytd_prep

)

SELECT

    snowball_key,
    customer_key,
    product_key,
    entity_key,
    other_key,
    revenue_type,
    month_roll,
    period_type,
    bop_arr,
    customer_churn,
    product_churn,
    deactivation,
    Intermittent_churn,
    downsell,
    -- downsell_price,
    -- downsell_volume,
    bop_arr + customer_churn + product_churn + downsell + deactivation + Intermittent_churn  AS grr,
    upsell,
    -- upsell_price,
    -- upsell_volume,
    cross_sell,
    bop_arr + customer_churn + product_churn + downsell + upsell + cross_sell AS nrr,
    new_customer,
    reactivation,
    winback,
    eop_arr,
    volume

FROM
    combined_period_type