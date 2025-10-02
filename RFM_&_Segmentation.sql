/*===============================================
    Creating RFM Table
-------------------------------------------------
 This will help us analysing customer behaviour and
 take out actionable insights about customers
=================================================*/

DROP TABLE IF EXISTS analytics.customer_rfm;
CREATE TABLE analytics.customer_rfm AS
WITH base AS (
    SELECT
        o.customer_unique_id,
        o.order_id,
        o.order_date,
        o.order_total
    FROM analytics.orders_clean o
    WHERE o.is_canceled_or_unavailable = FALSE
      AND o.customer_unique_id IS NOT NULL
),
rfm AS (
    SELECT
        customer_unique_id,
        (DATE '2018-10-17' - MAX(order_date)) AS recency_days, -- we used end date of snapshot instead of traditional current_date method, because we don't have real time data
        COUNT(DISTINCT order_id) AS frequency,
        SUM(order_total) AS monetary
    FROM base
    GROUP BY customer_unique_id
),
scored AS (
    SELECT
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        -- custom bins for recency
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 90 THEN 4
            WHEN recency_days <= 180 THEN 3
            WHEN recency_days <= 365 THEN 2
            ELSE 1
        END AS recency_score,
        -- custom bins for frequency
        CASE 
            WHEN frequency = 1 THEN 1
            WHEN frequency = 2 THEN 2
            WHEN frequency <= 4 THEN 3
            WHEN frequency <= 8 THEN 4
            ELSE 5
        END AS frequency_score,
        -- custom bins for monetary
        CASE 
            WHEN monetary <= 50 THEN 1
            WHEN monetary <= 200 THEN 2
            WHEN monetary <= 500 THEN 3
            WHEN monetary <= 1000 THEN 4
            ELSE 5
        END AS monetary_score
    FROM rfm
)
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score
FROM scored;

/*________________________________________________________
   
   We calulated RFM and created our 'customers_rfm' Table.
   we used custom bins here instead of 'NTILE()' method 
   because distributions was too skewed
___________________________________________________________*/

/*==========================================================
    Preparing For Segmentation
------------------------------------------------------------
 Before moving on to the segmentation we should explore what
 kind of customers we have in our data and how our RFM scores
 are distributed.
===========================================================*/


SELECT * FROM analytics.customer_rfm LIMIT(50);

SELECT DISTINCT(recency_score), SUM(recency_score) FROM analytics.customer_rfm
GROUP BY recency_score;

SELECT DISTINCT(monetary_score), SUM(monetary_score) FROM analytics.customer_rfm
GROUP BY monetary_score;

SELECT DISTINCT(frequency_score), SUM(frequency_score) FROM analytics.customer_rfm
GROUP BY frequency_score;

/*____________________________________________

   So from this basic query we can see that majority of customers
   are one time buyers and low paying, and recency are distributed normally.
   Keeping these distribution in mind we can do a proper segmentation.

_____________________________________________*/

/*==============================================
    Segmentation
------------------------------------------------
 This is step crucial for our customer behaviour
 analysis as we can group each type of customers
================================================*/

ALTER TABLE analytics.customer_rfm
ADD COLUMN segment TEXT;

UPDATE analytics.customer_rfm
SET segment = CASE
    -- Loyal: strong recency & frequency, plus decent or high monetary
    WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'loyal'
    -- One-time big spenders: purchased once but spent great
    WHEN frequency_score = 1 AND monetary_score >= 3 THEN 'one_time_big_spendors'
    -- Low-value one-timers: purchased once and spent low
    WHEN frequency_score = 1 AND monetary_score < 3 THEN 'low_value_one_timers'
    -- At-risk: used to purchase frequently but haven't seen recently
    WHEN frequency_score >= 3 AND recency_score <= 3 THEN 'at_risk'
    -- Rising star: purchased twice recently and has decent spent
    WHEN frequency_score = 2 AND recency_score >= 4 AND monetary_score >= 3 THEN 'rising_star'
    -- Lost: purchased twice but not seen recently
    WHEN frequency_score = 2 AND recency_score < 3 AND monetary_score >= 3 THEN 'lost'
    -- Everything else
    ELSE 'others'
END;

/*___________________________________________________________________

    Now we have Clean 7 Segment from which 'rising_star', 'at_risk'
    and 'lost' are our actionable segments.
    and segments like : 'loyal', 'low-value onetimers' and 'one-time big spenders'
    are just informational segments
____________________________________________________________________*/


-- Segmentation Summary Table
SELECT
    segment,
    COUNT(customer_unique_id) AS customer_count,
    SUM(monetary) AS total_revenue,
    ROUND(AVG(monetary),2) AS avg_order_total,
    ROUND(AVG(recency_days),2) AS avg_recency_days,
    ROUND(AVG(frequency),2) AS avg_frequency
FROM analytics.customer_rfm
GROUP BY segment
ORDER BY total_revenue DESC;

/*_________________________________________________________

    (final comment pending yet) no need to review this
_________________________________________________________*/
