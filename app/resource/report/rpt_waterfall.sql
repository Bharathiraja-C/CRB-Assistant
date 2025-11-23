WITH 


snowball AS (

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'ltm_recurring_revenue'    AS kpi,
        eop_arr  AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE eop_arr <> 0
        AND revenue_type ='Recurring'

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'ltm_re-occurring_revenue'    AS kpi,
         eop_arr AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE eop_arr <> 0
        AND revenue_type ='Re-occurring'

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'ltm_non_recurring_revenue'    AS kpi,
        eop_arr                        AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE eop_arr <> 0
    AND revenue_type ='Non-Recurring'

    UNION ALL
    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'Total_ltm_revenue'    AS kpi,
        eop_arr      AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE eop_arr <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'eop_arr'    AS kpi,
        eop_arr      AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE eop_arr <> 0

    UNION ALL

    SELECT 

        snowball_key  AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'bop_arr'    AS kpi,
        bop_arr      AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE bop_arr <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'customer_churn' AS kpi,
        customer_churn AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE customer_churn <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'new_customer' AS kpi,
        new_customer AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE new_customer <> 0

    UNION ALL

    SELECT 

        snowball_key  AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'cross_sell'  AS kpi,
        cross_sell    AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE cross_sell <> 0

    UNION ALL

    SELECT 

        snowball_key   AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'product_churn' AS kpi,
        product_churn   AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE product_churn <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'upsell'     AS kpi,
        upsell       AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE upsell <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'downsell' AS kpi,
        downsell AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE downsell <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'grr' AS kpi,
        grr AS kpi_value,
        revenue_type

    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE grr <> 0

    UNION ALL

    SELECT 

        snowball_key AS waterfall_key,
        customer_key,
        product_key,
        entity_key,
        other_key,
        month_roll,
        period_type,
        'nrr' AS kpi,
        nrr AS kpi_value,
        revenue_type
        
    FROM {{ ref('rpt_revenue_bridge') }}
    WHERE nrr <> 0

) 

SELECT * FROM snowball