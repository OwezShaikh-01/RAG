/*
===========================================================================
SECTION 1: CORE KPIs
---------------------------------------------------------------------------
Metrics: Gross GMV, Net Revenue, Avg Order Value, Total Orders, Repeat Rate
===========================================================================
*/

-- Gross GMV (all orders, even canceled/unavailable)
SELECT SUM(order_total) AS gross_gmv
FROM analytics.orders_clean;

-- Net Revenue (only valid orders)
SELECT SUM(order_total) AS net_revenue
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE;

-- Average Revenue per Order (AOV, valid only)
SELECT ROUND(SUM(order_total)::NUMERIC / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE;

-- Total Orders (valid only)
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE;

-- Repeat Rate (customers with >=2 valid orders)
WITH customer_orders AS (
    SELECT customer_unique_id, COUNT(DISTINCT order_id) AS order_count
    FROM analytics.orders_clean
    WHERE is_canceled_or_unavailable = FALSE
    GROUP BY customer_unique_id
)
SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE order_count >= 2) / COUNT(*), 2) AS repeat_rate_pct
FROM customer_orders;

-- Cancellation % (lost value)
SELECT 
    CAST(SUM(order_total) FILTER (WHERE is_canceled_or_unavailable = TRUE) * 100.0
    / SUM(order_total) AS DECIMAL(10,2)) AS cancel_rate_pct
FROM analytics.orders_clean;

-- GMV vs Net Revenue (monthly comparison with cancellation rates)
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(order_total) AS gross_gmv,
    SUM(order_total) FILTER (WHERE is_canceled_or_unavailable = FALSE) AS net_revenue,
    SUM(order_total) FILTER (WHERE is_canceled_or_unavailable = TRUE) AS canceled_value,
    ROUND(
        SUM(order_total) FILTER (WHERE is_canceled_or_unavailable = TRUE) * 100.0 
        / NULLIF(SUM(order_total), 0), 
        2
    ) AS cancel_rate_pct
FROM analytics.orders_clean
GROUP BY 1
ORDER BY 1;

/*__________________________________________________________________________
  Summary:
   GMV : 15.84M BRL
   Total Revenue : 15.73M BRL (we have 0.68% cancellation rate which is pretty low)
   Total Orders : 98k
   AOV : 160.23 BRL
   Repeat Rate : 3.04% (since the majority of our customers are one-time buyers)
__________________________________________________________________________*/

/*
======================================================================
SECTION 2: Delivery Performance
----------------------------------------------------------------------
Metrics: Avg Delivery Days, % Delayed Orders, Weekday vs Weekend
======================================================================
*/

-- Avg Delivery Days
SELECT ROUND(AVG(days_to_delivery), 2) AS avg_delivery_days
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE
  AND days_to_delivery IS NOT NULL;

-- % Delayed Orders
SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE is_delayed = TRUE) / COUNT(*), 2) AS delayed_orders_pct
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE;

-- Weekday vs Weekend Orders & Delivery
SELECT 
    CASE 
        WHEN EXTRACT(DOW FROM order_date) IN (0,6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(AVG(days_to_delivery),2) AS avg_delivery_days,
    ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)/COUNT(*),2) AS delayed_pct
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE
  AND days_to_delivery IS NOT NULL
GROUP BY day_type
ORDER BY day_type;

/*_____________________________________________________________________________

  Average delivery is about 12.5 days, which feels long. Around 6–7% of orders
  get delayed, which isn’t too bad. Weekends have slightly lower delayed orders
  since the load is also less.
_____________________________________________________________________________*/

/*
====================================================================
SECTION 3: State Performance
--------------------------------------------------------------------
Metrics: Revenue, Cancel %, Avg Delivery Days by State
====================================================================
*/

-- Revenue by State (normalized)
SELECT UPPER(TRIM(c.customer_state)) AS customer_state,
       SUM(o.order_total) AS net_revenue
FROM analytics.orders_clean o
JOIN analytics.dim_customers c USING (customer_unique_id)
WHERE o.is_canceled_or_unavailable = FALSE
GROUP BY UPPER(TRIM(c.customer_state))
ORDER BY net_revenue DESC;

-- Cancel Rate by State
SELECT UPPER(TRIM(c.customer_state)) AS customer_state,
       SUM(o.order_total) FILTER (WHERE o.is_canceled_or_unavailable = TRUE) AS canceled_value,
       SUM(o.order_total) AS gross_gmv,
       ROUND(SUM(o.order_total) FILTER (WHERE o.is_canceled_or_unavailable = TRUE) * 100.0 / NULLIF(SUM(o.order_total),0), 2) AS cancel_rate_pct
FROM analytics.orders_clean o
JOIN analytics.dim_customers c USING (customer_unique_id)
GROUP BY UPPER(TRIM(c.customer_state))
ORDER BY cancel_rate_pct DESC;

-- Avg Delivery Days by State (valid only)
SELECT UPPER(TRIM(c.customer_state)) AS customer_state,
       ROUND(AVG(o.days_to_delivery), 2) AS avg_delivery_days
FROM analytics.orders_clean o
JOIN analytics.dim_customers c USING (customer_unique_id)
WHERE o.is_canceled_or_unavailable = FALSE
  AND o.days_to_delivery IS NOT NULL
GROUP BY UPPER(TRIM(c.customer_state))
ORDER BY avg_delivery_days;


/*___________________________________________________________________________
  
  São Paulo, Rio de Janeiro, and Minas Gerais carry most of the revenue.
  São Paulo customers get their deliveries in under 9 days, while the North/Northeast
  customers wait 20–30 days.
  Cancellations are low overall, but states like Goiás and Roraima seem to have
  higher drop-offs.
__________________________________________________________________________*/


/* 
=======================================================================
SECTION 4: Product & Category Performance
-----------------------------------------------------------------------
Metrics: Top Categories, Avg Review Score, Cancel Rate
=======================================================================
 */

-- Top Categories by Net Revenue
SELECT ia.primary_category,
       SUM(o.order_total) AS net_revenue
FROM analytics.orders_clean o
JOIN analytics.order_items_agg ia USING (order_id)   -- aggregated table prevents double count
WHERE o.is_canceled_or_unavailable = FALSE
GROUP BY ia.primary_category
ORDER BY net_revenue DESC
LIMIT 10;

-- Avg Review Score by Category
SELECT ia.primary_category,
       ROUND(AVG(r.review_score), 2) AS avg_review_score
FROM analytics.order_reviews_dedup r
JOIN analytics.orders_clean o USING (order_id)
JOIN analytics.order_items_agg ia USING (order_id)
WHERE o.is_canceled_or_unavailable = FALSE
GROUP BY ia.primary_category
ORDER BY avg_review_score DESC;

-- Cancel Rate by Category
SELECT ia.primary_category,
       SUM(o.order_total) FILTER (WHERE o.is_canceled_or_unavailable = TRUE) AS canceled_value,
       SUM(o.order_total) AS gross_gmv,
       ROUND(SUM(o.order_total) FILTER (WHERE o.is_canceled_or_unavailable = TRUE) * 100.0 / NULLIF(SUM(o.order_total),0), 2) AS cancel_rate_pct
FROM analytics.orders_clean o
JOIN analytics.order_items_agg ia USING (order_id)
GROUP BY ia.primary_category
ORDER BY cancel_rate_pct DESC;

/*__________________________________________________________________________

  Categories like Beauty, gifts, and bed/bath give most revenue.
  Reviews are better in categories like 'cool_stuff', small appliances,
  sports, health_beauty, etc. The DVDs category has the highest cancellation rate
  (20.34%), while other categories have a cancellation rate < 3%. 
__________________________________________________________________________*/


/*
=========================================================================
SECTION 5: Seller Performance & Payments
-------------------------------------------------------------------------
Metrics: Top Sellers, Delivery Performance, Payment Types
=========================================================================
*/

-- Top Sellers by Net Revenue
SELECT 
    s.seller_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price + oi.freight_value) AS total_revenue,
    ROUND(AVG(o.days_to_delivery),2) AS avg_delivery_days
FROM analytics.orders_clean o
JOIN analytics.order_items_agg oi USING (order_id)   -- use agg to avoid duplicate items
JOIN analytics.dim_sellers s USING (seller_id)
WHERE o.is_canceled_or_unavailable = FALSE
GROUP BY s.seller_id
ORDER BY total_revenue DESC
LIMIT 20;

-- Payment Type Distribution (valid only)
SELECT primary_payment_type,
       COUNT(*) AS num_orders,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) AS pct_orders
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE
GROUP BY primary_payment_type
ORDER BY num_orders DESC;

-- Payment Type & Revenue (valid only)
SELECT 
    primary_payment_type,
    ROUND(SUM(order_total),2) AS total_revenue,
    ROUND(SUM(order_total)/NULLIF(COUNT(DISTINCT order_id),0),2) AS avg_order_value,
    ROUND(100.0 * SUM(order_total)/SUM(SUM(order_total)) OVER(),2) AS pct_of_total_revenue
FROM analytics.orders_clean
WHERE is_canceled_or_unavailable = FALSE
GROUP BY primary_payment_type
ORDER BY total_revenue DESC;


/*________________________________________________________________

  Credit cards are the most preferred payment method. They have the most orders,
  most revenue, and higher basket sizes.
  Boleto holds the second position with lots of smaller values.
  Debit and vouchers don’t make any great impact.

  Top sellers each make ~200–250k BRL, but no one dominates.
  Some sellers have huge volumes of sales, and others hit the same revenue
  with fewer big-ticket sales.
  Most sellers deliver in 11–15 days, but a few of them take 20+ days and drag
  the averages down.

________________________________________________________________*/