WITH

product_grew AS (

    SELECT

        period_revenue_key
        , customer_key
        , customer_level_1
        --, customer_level_2
        , product_key
        , product_level_1
        --, product_level_2
        , entity_key
        , other_key
        , arr
        , arr_lm
        , arr_l3m
        , arr_ltm
        , arr_ytd
        , month_roll
        , revenue_type

        --MONTHLY GREW
        , CASE
            WHEN sum_arr_lm_delta > 0
            THEN 1
            ELSE 0
        END AS product_grew_monthly,

        CASE
            WHEN sum_arr_lm_delta < 0
            THEN 1
            ELSE 0
        END AS product_declined_monthly,

        --QUARTERLY GREW
        CASE
            WHEN sum_arr_l3m_delta > 0
            THEN 1
            ELSE 0
        END AS product_grew_quarterly,

        CASE
            WHEN sum_arr_l3m_delta < 0
            THEN 1
            ELSE 0
        END AS product_declined_quarterly,

        --YEARLY GREW
        CASE
            WHEN sum_arr_ltm_delta > 0
            THEN 1
            ELSE 0
        END AS product_grew_yearly,

        CASE
            WHEN sum_arr_ltm_delta < 0
            THEN 1
            ELSE 0
        END AS product_declined_yearly,

        --YTD GREW
        CASE
            WHEN sum_arr_ytd_delta > 0
            THEN 1
            ELSE 0
        END AS product_grew_ytd,

        CASE
            WHEN sum_arr_ytd_delta < 0
            THEN 1
            ELSE 0
        END AS product_declined_ytd

    FROM {{ ref('period_revenue') }}

), 

ranked_product AS (

    SELECT

        p1.period_revenue_key
        , p1.customer_key
        , p1.customer_level_1
        --, p1.customer_level_2
        , p1.product_key
        , p1.product_level_1
        -- p1.product_level_2
        , p1.entity_key
        , p1.other_key
        , p1.arr
        , p1.arr_lm
        , p1.arr_l3m
        , p1.arr_ltm
        , p1.arr_ytd
        , p1.month_roll
        , p1.revenue_type
        , c.lm_customer_new_flag
        , c.l3m_customer_new_flag
        , c.ltm_customer_new_flag
        , c.ytd_customer_new_flag
        , c.lm_customer_churn_flag
        , c.l3m_customer_churn_flag
        , c.ltm_customer_churn_flag
        , c.ytd_customer_churn_flag
        --- MONTHLY FLAGS
        , CASE
            WHEN lm_customer_existing_flag=1
            AND product_start_month = p1.month_roll
                THEN 1
            ELSE 0
        END AS lm_cross_sell_flag,

        CASE
            WHEN product_grew_monthly = 1
            AND lm_product_existing_flag = 1
                THEN 1 
            ELSE 0 
        END AS lm_upsell_flag,

        CASE 
            WHEN product_declined_monthly = 1
            AND lm_product_existing_flag = 1
                THEN 1 
            ELSE 0 
        END AS lm_downsell_flag,
        
        -- QUARTERLY FLAGS
        CASE
            WHEN l3m_customer_existing_flag=1      
            AND l3m_product_existing_flag=0
                THEN 1 
            ELSE 0 
        END AS l3m_cross_sell_flag,

        CASE 
            WHEN product_grew_quarterly = 1
            AND l3m_product_existing_flag=1
                THEN 1 
            ELSE 0 
        END AS l3m_upsell_flag,

        CASE 
            WHEN product_declined_quarterly = 1
            AND l3m_product_existing_flag=1
                THEN 1 
            ELSE 0 
        END AS l3m_downsell_flag,

        -- YEARLY FLAGS
        CASE
            WHEN ltm_customer_existing_flag=1  
            AND ltm_product_existing_flag=0
                THEN 1 
            ELSE 0 
        END AS ltm_cross_sell_flag,

        CASE 
            WHEN product_grew_yearly = 1
            AND ltm_product_existing_flag=1
                THEN 1 
            ELSE 0 
        END AS ltm_upsell_flag,

        CASE 
            WHEN product_declined_yearly = 1
            AND ltm_product_existing_flag=1
                THEN 1 
            ELSE 0 
        END AS ltm_downsell_flag,

        -- YTD FLAGS
        CASE
            WHEN ytd_customer_existing_flag=1   
            AND ytd_product_existing_flag=0
                THEN 1 
            ELSE 0 
        END AS ytd_cross_sell_flag,

        CASE 
            WHEN product_grew_ytd = 1
            AND ytd_product_existing_flag=1
                THEN 1 
            ELSE 0 
        END AS ytd_upsell_flag,

        CASE 
            WHEN product_declined_ytd = 1
            AND ytd_product_existing_flag=1
                THEN 1 
            ELSE 0 
        END AS ytd_downsell_flag

    FROM product_grew AS p1

    INNER JOIN
        {{ ref('customer_lifecycle_events') }} AS c
    ON p1.period_revenue_key = c.customer_lifecycle_events_key
        AND p1.month_roll = c.month_roll

    INNER JOIN
        {{ ref('customer_product_lifecycle_events') }} AS p2
    ON p1.period_revenue_key = p2.customer_product_lifecycle_events_key
        AND p1.month_roll =  p2.month_roll

    INNER JOIN
        {{ ref('customer_product_contract') }} AS p3
    ON p1.customer_level_1 = p3.customer_level_1
        AND p1.product_level_1 = p3.product_level_1

),
-- Logic for Intermittent_churn , Winback , deactivation, reactivaiton helper columns
find_next_nonzero_month AS (

    SELECT 
    
        current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month -- finding next non zero month, so can get possible winback dates

    FROM ranked_product AS current_plan

    LEFT JOIN 
      ranked_product AS next_plan 
    ON current_plan.customer_level_1 = next_plan.customer_level_1 
        AND next_plan.arr != 0 
        AND next_plan.month_roll > current_plan.month_roll

    WHERE  current_plan.lm_customer_new_flag = 0

    GROUP BY 
            current_plan.customer_level_1
            , current_plan.month_roll

),

find_prev_nonzero_month AS (

    SELECT DISTINCT 
        
         current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month  -- will get possible intermittent churn dates
    
    FROM ranked_product AS current_plan

    LEFT JOIN 
        ranked_product AS prev_plan 
    ON current_plan.customer_level_1 = prev_plan.customer_level_1
        AND prev_plan.arr <> 0
        AND prev_plan.month_roll < current_plan.month_roll
        

    GROUP BY 
        current_plan.customer_level_1
        , current_plan.month_roll

),

find_next_nonzero_month_l3m AS (

    SELECT 
    
        current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month_l3m -- finding next non zero month, so can get possible winback dates

    FROM ranked_product AS current_plan

    LEFT JOIN 
      ranked_product AS next_plan 
    ON current_plan.customer_level_1 = next_plan.customer_level_1 
        AND next_plan.arr_l3m != 0 
        AND next_plan.month_roll > current_plan.month_roll

    WHERE  current_plan.l3m_customer_new_flag = 0

    GROUP BY 
        current_plan.customer_level_1
        , current_plan.month_roll

),

find_prev_nonzero_month_l3m AS (

    SELECT DISTINCT 
        
         current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month_l3m  -- will get possible intermittent churn dates
    
    FROM ranked_product AS current_plan

    LEFT JOIN 
        ranked_product AS prev_plan 
    ON current_plan.customer_level_1 = prev_plan.customer_level_1
        AND prev_plan.arr_l3m <> 0
        AND prev_plan.month_roll < current_plan.month_roll

    GROUP BY 
        current_plan.customer_level_1
        , current_plan.month_roll
),

find_next_nonzero_month_ltm AS (

    SELECT 

         current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month_ltm -- finding next non zero month, so can get possible winback dates

    FROM ranked_product AS current_plan

    LEFT JOIN
       ranked_product AS next_plan 
    ON current_plan.customer_level_1 = next_plan.customer_level_1 
        AND next_plan.arr_ltm != 0 
        AND next_plan.month_roll > current_plan.month_roll

    WHERE  current_plan.ltm_customer_new_flag = 0
    
    GROUP BY 
            current_plan.customer_level_1
            , current_plan.month_roll
            
),


find_prev_nonzero_month_ltm AS (

    SELECT DISTINCT 
    
         current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month_ltm  -- will get possible intermittent churn dates

    FROM ranked_product AS current_plan

    LEFT JOIN 
        ranked_product AS prev_plan 
    ON current_plan.customer_level_1 = prev_plan.customer_level_1 
        AND prev_plan.arr_ltm <> 0
        AND prev_plan.month_roll < current_plan.month_roll

    GROUP BY 
        current_plan.customer_level_1
        , current_plan.month_roll

),

find_next_nonzero_month_ytd AS (

    SELECT 

         current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month_ytd -- finding next non zero month, so can get possible winback dates

    FROM ranked_product AS current_plan

    LEFT JOIN
       ranked_product AS next_plan 
    ON current_plan.customer_level_1 = next_plan.customer_level_1 
        AND next_plan.arr_ytd != 0 
        AND next_plan.month_roll > current_plan.month_roll

    WHERE  current_plan.ytd_customer_new_flag = 0
    
    GROUP BY 
            current_plan.customer_level_1
            , current_plan.month_roll

),


find_prev_nonzero_month_ytd AS (

    SELECT DISTINCT 
    
         current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month_ytd  -- will get possible intermittent churn dates

    FROM ranked_product AS current_plan

    LEFT JOIN 
        ranked_product AS prev_plan 
    ON current_plan.customer_level_1 = prev_plan.customer_level_1 
        AND prev_plan.arr_ytd <> 0
        AND prev_plan.month_roll < current_plan.month_roll

    GROUP BY 
        current_plan.customer_level_1
        , current_plan.month_roll

),

pre_final AS (

    SELECT 
    
         rp.*
        , fn.next_nonzero_month 
        , fp.prev_nonzero_month
        , fn3m.next_nonzero_month_l3m
        , fp3m.prev_nonzero_month_l3m
        , fnm.next_nonzero_month_ltm
        , fpm.prev_nonzero_month_ltm
        , fntd.next_nonzero_month_ytd
        , fptd.prev_nonzero_month_ytd
    
    FROM ranked_product  AS rp                                    -- will get possible dates of churn winback in one table

    LEFT JOIN 
        find_next_nonzero_month AS fn 
    ON rp.customer_level_1=fn.customer_level_1
        AND rp.month_roll=fn.month_roll

    LEFT JOIN 
        find_prev_nonzero_month AS fp
    ON rp.customer_level_1=fp.customer_level_1
        AND rp.month_roll=fp.month_roll
    
    LEFT JOIN 
        find_next_nonzero_month_l3m AS fn3m 
    ON rp.customer_level_1=fn3m.customer_level_1
        AND rp.month_roll=fn3m.month_roll

    LEFT JOIN 
        find_prev_nonzero_month_l3m AS fp3m
    ON rp.customer_level_1=fp3m.customer_level_1
        AND rp.month_roll=fp3m.month_roll

    LEFT JOIN 
        find_next_nonzero_month_ltm AS fnm
    ON rp.customer_level_1=fnm.customer_level_1
        AND rp.month_roll=fnm.month_roll

    LEFT JOIN 
        find_prev_nonzero_month_ltm AS fpm
    ON rp.customer_level_1=fpm.customer_level_1
        AND rp.month_roll=fpm.month_roll

    LEFT JOIN 
        find_next_nonzero_month_ytd AS fntd
    ON rp.customer_level_1=fntd.customer_level_1
        AND rp.month_roll=fntd.month_roll

    LEFT JOIN 
        find_prev_nonzero_month_ytd AS fptd
    ON rp.customer_level_1=fptd.customer_level_1
        AND rp.month_roll=fptd.month_roll

),

customer_product_revenue_events AS(

    SELECT

             *
    --creating a flag column bsaed on conditions with date difference
        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) = 0  
                AND DATEDIFF(MONTH,prev_nonzero_month ::DATE,next_nonzero_month ::DATE)-2 = 1
            THEN 1
            ELSE 0
         END AS deactivation_helper
        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0 
                AND DATEDIFF(MONTH,prev_nonzero_month ::DATE,month_roll ::DATE)-2 = 1
            THEN 1
            ELSE 0
         END AS reactivation_helper
        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) =0  
                 AND DATEDIFF(MONTH,prev_nonzero_month ::DATE,next_nonzero_month ::DATE)-1> 3
            THEN 1
            ELSE 0
         END AS intermittent_churn_helper
        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0  
                AND DATEDIFF(MONTH,prev_nonzero_month ::DATE,month_roll ::DATE)-1 > 3
            THEN 1
            ELSE 0
         END AS winback_helper
         , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0  
                AND (SUM(ARR_L3M) OVER (PARTITION BY customer_level_1,month_roll))  = 0  
                AND DATEDIFF(MONTH,prev_nonzero_month_l3m ::DATE,next_nonzero_month_l3m ::DATE) -1 > 3
            THEN 1
            ELSE 0
         END AS l3m_winback_helper
        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0 
                AND (SUM(ARR_L3M) OVER (PARTITION BY customer_level_1,month_roll))  = 0 
                AND DATEDIFF(MONTH,prev_nonzero_month_l3m ::DATE,next_nonzero_month_l3m ::DATE) -2 = 1
            THEN 1
            ELSE 0
         END AS l3m_reactivation_helper

        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0  
                AND (SUM(ARR_LTM) OVER (PARTITION BY customer_level_1,month_roll))  = 0  
                AND DATEDIFF(MONTH,prev_nonzero_month_ltm ::DATE,next_nonzero_month_ltm ::DATE) -1 > 3
            THEN 1
            ELSE 0
         END AS ltm_winback_helper

        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0 
                AND (SUM(ARR_LTM) OVER (PARTITION BY customer_level_1,month_roll))  = 0 
                AND DATEDIFF(MONTH,prev_nonzero_month_ltm ::DATE,next_nonzero_month_ltm ::DATE) -2 = 1
            THEN 1
            ELSE 0
         END AS ltm_reactivation_helper

         , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0  
                AND (SUM(ARR_YTD) OVER (PARTITION BY customer_level_1,month_roll))  = 0  
                AND DATEDIFF(MONTH,prev_nonzero_month_ytd ::DATE,next_nonzero_month_ytd ::DATE) -1 > 3
            THEN 1
            ELSE 0
         END AS ytd_winback_helper

        , CASE
            WHEN (SUM(ARR) OVER (PARTITION BY customer_level_1,month_roll)) <>0 
                AND (SUM(ARR_YTD) OVER (PARTITION BY customer_level_1,month_roll))  = 0 
                AND DATEDIFF(MONTH,prev_nonzero_month_ytd ::DATE,next_nonzero_month_ytd ::DATE) -2 = 1
            THEN 1
            ELSE 0
         END AS ytd_reactivation_helper

    FROM pre_final  

)


SELECT

    period_revenue_key AS customer_product_revenue_events_key,
    customer_key,
    customer_level_1,
    -- customer_level_2,
    product_key,
    product_level_1,
    -- product_level_2,
    entity_key,
    other_key,
    month_roll,
    revenue_type,

    winback_helper,
    deactivation_helper,
    reactivation_helper,
    intermittent_churn_helper,

    CASE 
        WHEN  winback_helper = 0  AND deactivation_helper = 0
            AND reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN lm_cross_sell_flag
        ELSE 0
    END lm_cross_sell_flag,
     CASE 
        WHEN  winback_helper = 0  AND deactivation_helper = 0
            AND reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN lm_upsell_flag
        ELSE 0
    END lm_upsell_flag,
     CASE 
        WHEN  winback_helper = 0  AND deactivation_helper = 0
            AND reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN lm_downsell_flag
        ELSE 0
    END lm_downsell_flag,

    l3m_reactivation_helper,
    l3m_winback_helper,
     CASE 
        WHEN  l3m_winback_helper = 0  AND deactivation_helper = 0
            AND l3m_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN l3m_cross_sell_flag
        ELSE 0
    END l3m_cross_sell_flag,
     CASE 
        WHEN  l3m_winback_helper = 0  AND deactivation_helper = 0
            AND l3m_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN l3m_upsell_flag
        ELSE 0
    END l3m_upsell_flag,
     CASE 
        WHEN  l3m_winback_helper = 0  AND deactivation_helper = 0
            AND l3m_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN l3m_downsell_flag
        ELSE 0
    END l3m_downsell_flag,

    ltm_reactivation_helper,
    ltm_winback_helper,
     CASE 
        WHEN  ltm_winback_helper = 0  AND deactivation_helper = 0
            AND ltm_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN ltm_cross_sell_flag
        ELSE 0
    END ltm_cross_sell_flag,
     CASE 
        WHEN  ltm_winback_helper = 0  AND deactivation_helper = 0
            AND ltm_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN ltm_upsell_flag
        ELSE 0
    END ltm_upsell_flag,
     CASE 
        WHEN  ltm_winback_helper = 0  AND deactivation_helper = 0
            AND ltm_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN ltm_downsell_flag
        ELSE 0
    END ltm_downsell_flag,

    ytd_reactivation_helper,
    ytd_winback_helper,
     CASE 
        WHEN  ytd_winback_helper = 0  AND deactivation_helper = 0
            AND ytd_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN ytd_cross_sell_flag
        ELSE 0
    END ytd_cross_sell_flag,
     CASE 
        WHEN  ytd_winback_helper = 0  AND deactivation_helper = 0
            AND ytd_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN ytd_upsell_flag
        ELSE 0
    END ytd_upsell_flag,
     CASE 
        WHEN  ytd_winback_helper = 0  AND deactivation_helper = 0
            AND ytd_reactivation_helper = 0 AND intermittent_churn_helper = 0 
        THEN ytd_downsell_flag
        ELSE 0
    END ytd_downsell_flag

FROM
    customer_product_revenue_events