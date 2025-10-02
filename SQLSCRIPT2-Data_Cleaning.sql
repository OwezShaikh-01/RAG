-- 0. Setup
CREATE SCHEMA IF NOT EXISTS analytics; -- cleaned analytics layer
CREATE EXTENSION IF NOT EXISTS unaccent;
/*__________________________________________________________________

 We're creating a separate schema for analysis so raw data stays untouched
 and the EXTENSION unaccent is necessary for normalizing cities in our data
__________________________________________________________________*/


/*
======================================================
1) DIM_GEO: normalize geolocation, median lat/lng per zip
------------------------------------------------------
We will create a stable geolocation dimension aggregated at
the cleaned zip_prefix + normalized city level.
We'll pick median lat/lng to avoid extreme outliers, collect distinct states,
and surface whether a zip prefix maps to more than one state.
======================================================
*/
DROP TABLE IF EXISTS analytics.dim_geolocation CASCADE;

CREATE TABLE analytics.dim_geolocation AS
WITH city AS (
  SELECT
    geolocation_zip_code_prefix::INT AS zip_prefix,
    unaccent(LOWER(TRIM(geolocation_city))) AS city,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY geolocation_lat) AS lat_med,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY geolocation_lng) AS lng_med,
    ARRAY_AGG(DISTINCT geolocation_state) FILTER (WHERE geolocation_state IS NOT NULL) AS states,
    COUNT(*) AS count
  FROM public.olist_geolocation
  GROUP BY 1, 2
)
SELECT
  zip_prefix,
  city,
  lat_med  AS geolocation_lat,
  lng_med  AS geolocation_lng,
  states,
  count,
  CASE WHEN array_length(states,1) > 1 THEN TRUE ELSE FALSE END AS zip_prefix_ambiguous,
  (SELECT g.geolocation_state
   FROM public.olist_geolocation g
   WHERE g.geolocation_zip_code_prefix::INT = t.zip_prefix   -- FIXED here
   GROUP BY g.geolocation_state
   ORDER BY COUNT(*) DESC NULLS LAST
   LIMIT 1) AS dominant_state
FROM (
  SELECT zip_prefix, city, lat_med, lng_med, count, states,
         ROW_NUMBER() OVER (PARTITION BY zip_prefix ORDER BY count DESC) rn
  FROM city
) t
WHERE rn = 1;

CREATE INDEX ON analytics.dim_geolocation (zip_prefix);

/*_____________________________________________________________________________________

   We cleaned city names (removed accents, lowercased, trimmed spaces) so different spellings
   end up grouped together. Without this, "São Paulo" and "sao paulo" would split into two
   separate cities, leading to double counting in geo analysis.
   We take the median latitude/longitude per city+zip to avoid skew from outliers.
   We also keep all states tied to a zip and flag ambiguous ones, so analysis doesn’t
   quietly mis-assign customers/sellers to the wrong state.
_______________________________________________________________________________________*/



/*
======================================================
2) DIM_CUSTOMERS: canonical customers by customer_unique_id
------------------------------------------------------
creating a clean & unified view of customers
======================================================
*/

DROP TABLE IF EXISTS analytics.dim_customers CASCADE;

CREATE TABLE analytics.dim_customers AS
WITH c AS (
  SELECT
    customer_id,
    customer_unique_id,
    LPAD(CAST(customer_zip_code_prefix AS TEXT), 5, '0') AS zip_prefix,
    LOWER(TRIM(unaccent(customer_city))) AS customer_city,
    LOWER(TRIM(unaccent(customer_state))) AS customer_state
  FROM public.olist_customers
)
SELECT
  customer_unique_id,
  MIN(customer_id) AS canonical_customer_id,
  (ARRAY_AGG(DISTINCT zip_prefix) FILTER (WHERE zip_prefix IS NOT NULL))[1] AS zip_prefix,
  (ARRAY_AGG(DISTINCT customer_city) FILTER (WHERE customer_city IS NOT NULL))[1] AS customer_city,
  (ARRAY_AGG(DISTINCT customer_state) FILTER (WHERE customer_state IS NOT NULL))[1] AS customer_state,
  COUNT(DISTINCT customer_id) AS customer_id_variants
FROM c
GROUP BY customer_unique_id;

CREATE INDEX ON analytics.dim_customers (customer_unique_id);

/*_____________________________________________________________________________________

   We use customer_unique_id as the true identifier for a person, since one person can have
   multiple customer_id values. Canonicalizing avoids inflating customer counts and ensures
   metrics like repeat rate are accurate.
   We assign one representative ID, and store one consistent city/state/zip snapshot per
   unique customer. We also keep a count of duplicate IDs so we can monitor data quality.
_______________________________________________________________________________________*/



/*
======================================================
3) DIM_PRODUCTS: normalized product attributes + density flag
------------------------------------------------------
We’ll build a product dimension that includes details like the product’s volume, density, and some validity checks.
For density, we use a very cautious threshold so anything unusual gets flagged for review.
======================================================
*/

DROP TABLE IF EXISTS analytics.dim_products CASCADE;
CREATE TABLE analytics.dim_products AS
SELECT
  p.product_id,
  COALESCE(t.product_category_name_english, 'other') AS product_category,
  p.product_name_lenght,
  p.product_description_lenght,
  p.product_photos_qty,
  -- clean numeric values (convert 0 to NULL)
  NULLIF(p.product_weight_g, 0) AS product_weight_g,
  NULLIF(p.product_length, 0) AS product_length,
  NULLIF(p.product_height_cm, 0) AS product_height_cm,
  NULLIF(p.product_width_cm, 0)  AS product_width_cm,
  -- computed
  CASE
    WHEN p.product_length > 0 AND p.product_height_cm > 0 AND p.product_width_cm > 0
      THEN (p.product_length * p.product_height_cm * p.product_width_cm)
    ELSE NULL
  END AS volume_cm3,
  CASE
    WHEN p.product_weight_g > 0 AND p.product_length > 0 AND p.product_height_cm > 0 AND p.product_width_cm > 0
      THEN p.product_weight_g / (p.product_length * p.product_height_cm * p.product_width_cm)
    ELSE NULL
  END AS density_g_per_cm3,
  -- flags
  (CASE WHEN p.product_weight_g IS NULL OR p.product_weight_g <= 0 THEN TRUE ELSE FALSE END) AS product_weight_invalid,
  (CASE WHEN p.product_length IS NULL OR p.product_length <= 0
          OR p.product_height_cm IS NULL OR p.product_height_cm <= 0
          OR p.product_width_cm IS NULL OR p.product_width_cm <= 0 THEN TRUE ELSE FALSE END) AS product_dim_invalid,
  (CASE WHEN t.product_category_name IS NULL THEN TRUE ELSE FALSE END) AS category_missing_translation,
  (CASE WHEN p.product_weight_g > 0
              AND p.product_length > 0
              AND p.product_height_cm > 0
              AND p.product_width_cm > 0
              AND (p.product_weight_g / (p.product_length * p.product_height_cm * p.product_width_cm)) > 10
        THEN TRUE ELSE FALSE END) AS density_outlier
FROM public.olist_products p
LEFT JOIN public.product_category_name_translation t
  ON p.product_category_name = t.product_category_name;
CREATE INDEX ON analytics.dim_products (product_id);


/*_____________________________________________________________________________________

   We normalized numeric columns (turned 0 into NULL) to prevent fake values from entering
   calculations. Volume is computed only when all dimensions are valid, and density comes
   from weight ÷ volume.
   We flag outliers or invalid rows because they would distort GMV, freight analysis,
   and category-level insights if left unfiltered. These flags let us filter bad data
   without fully deleting it.
_______________________________________________________________________________________*/



/*
======================================================
4) DIM_SELLERS: normalize seller location and join geolocation
------------------------------------------------------
Create seller dimension enriched with geolocation lookup and ambiguous-zip flag.
======================================================
*/

DROP TABLE IF EXISTS analytics.dim_sellers CASCADE;
CREATE TABLE analytics.dim_sellers AS
SELECT
  s.seller_id,
  LPAD(CAST(seller_zip_code_prefix AS TEXT), 5, '0') AS zip_prefix,
  LOWER(TRIM(unaccent(seller_city))) AS seller_city,
  LOWER(TRIM(unaccent(seller_state))) AS seller_state,
  g.geolocation_lat,
  g.geolocation_lng,
  g.zip_prefix_ambiguous
FROM public.olist_sellers s
LEFT JOIN analytics.dim_geolocation g
  ON (LPAD(CAST(seller_zip_code_prefix AS TEXT),5,'0')::INT = g.zip_prefix);

CREATE INDEX ON analytics.dim_sellers (seller_id);

/*_____________________________________________________________________________________

   We cleaned seller city names and mapped them to geo dimension data. This ensures
   sellers align with the same geographic standards as customers, enabling valid
   seller-to-customer distance or regional performance analysis.
   If a seller’s zip doesn’t match, it means that seller can’t be placed accurately
   on the map. That would distort state or city-level performance comparisons.
_______________________________________________________________________________________*/



/*
======================================================
5) REVIEW DEDUPE + FLAGS
------------------------------------------------------
Deduplicate reviews by review_id and add derived flags (length, presence of text, low-quality).
We choose the most recent answer/creation time and higher score when duplicates exist.
======================================================
*/

DROP TABLE IF EXISTS analytics.order_reviews_dedup CASCADE;
CREATE TABLE analytics.order_reviews_dedup AS
SELECT *
FROM (
  SELECT r.*,
    ROW_NUMBER() OVER (
      PARTITION BY review_id
      ORDER BY COALESCE(review_answer_timestamp, review_creation_date) DESC, review_score DESC
    ) rn
  FROM public.olist_order_reviews r
) t
WHERE rn = 1;

ALTER TABLE analytics.order_reviews_dedup
ADD COLUMN review_length INT GENERATED ALWAYS AS (char_length(coalesce(review_comment_message,''))) STORED,
ADD COLUMN review_has_text BOOLEAN GENERATED ALWAYS AS ((coalesce(review_comment_message,'') ~ '\S')::BOOLEAN) STORED,
ADD COLUMN low_quality_review BOOLEAN GENERATED ALWAYS AS ((char_length(coalesce(review_comment_message,'')) < 5) OR (coalesce(review_comment_message,'') = '')) STORED;

/*_____________________________________________________________________________________

   We deduplicated reviews by keeping the latest review (answer/creation time) and preferring
   the higher score if there’s a tie. This avoids double-counting while still capturing the
   most reliable customer signal.
   Flags like "low_quality_review" help filter out noise (e.g., “ok” or empty reviews),
   so text/sentiment analysis and score distributions aren’t diluted by junk data.
_______________________________________________________________________________________*/



/*
======================================================
6) ORDER_ITEMS flags + aggregates
------------------------------------------------------
Flag invalid monetary values at item level, then aggregate per-order for a canonical item summary.
======================================================
*/
DROP TABLE IF EXISTS analytics.order_items_flags CASCADE;
CREATE TABLE analytics.order_items_flags AS
SELECT
  oi.*,
  CASE WHEN oi.price <= 0 OR oi.price IS NULL THEN TRUE ELSE FALSE END AS invalid_price,
  CASE WHEN oi.freight_value <= 0 OR oi.freight_value IS NULL THEN TRUE ELSE FALSE END AS invalid_freight
FROM public.olist_order_items oi;

DROP TABLE IF EXISTS analytics.order_items_agg CASCADE;
CREATE TABLE analytics.order_items_agg AS
SELECT
  order_id,
  COUNT(*) AS items_count,
  SUM(CASE WHEN NOT invalid_price THEN price ELSE 0 END) AS sum_item_price,
  SUM(CASE WHEN NOT invalid_freight THEN freight_value ELSE 0 END) AS sum_freight_value,
  MIN(shipping_limit_date) AS min_shipping_limit_date,
  MIN(dp.product_category) AS primary_category
FROM analytics.order_items_flags oi
LEFT JOIN analytics.dim_products dp USING (product_id)
GROUP BY order_id;

/*_____________________________________________________________________________________

   This step matters because GMV, AOV, and revenue analysis depend on clean totals.
   Invalid rows would directly distort financial KPIs and downstream dashboards.
   We flagged invalid prices/freight at item level so they don’t leak into order totals.
   At the order level, we aggregated items into a canonical order summary (count, price, freight).
_______________________________________________________________________________________*/



/*
======================================================
7) PAYMENTS summary
------------------------------------------------------
Summarize payment rows per order and flag invalid payment rows.
Normalize 'not_defined' payment type to 'unknown' for grouping.
======================================================
*/

DROP TABLE IF EXISTS analytics.order_payments_summary CASCADE;
CREATE TABLE analytics.order_payments_summary AS
SELECT
  op.order_id,
  SUM(CASE WHEN op.payment_value > 0 THEN op.payment_value ELSE 0 END)::NUMERIC AS sum_payments,
  MIN(LOWER(op.payment_type)) AS primary_payment_type,
  MAX(payment_installments) AS max_installments,
  COUNT(*) AS payment_rows,
  SUM(CASE WHEN op.payment_value <= 0 OR op.payment_value IS NULL THEN 1 ELSE 0 END) AS invalid_payment_rows
FROM public.olist_order_payments op
GROUP BY op.order_id;

/*_____________________________________________________________________________________
   
   payments must reconcile with order totals (GMV vs. actual revenue). If we let invalid
   payments through, our financial KPIs would misreport the actual cash flow.
   So we aggregated payments per order, keeping only positive values and normalizing payment types.
   Invalid/zero rows are flagged for review. 
_______________________________________________________________________________________*/



/*
======================================================
8) ORDERS_CLEAN (canonical fact table: one row per order)
------------------------------------------------------
Build the canonical order fact by joining cleaned dims and aggregates.
Compute delivery, chronology, cancel, and payment coverage metrics for downstream analysis.
======================================================
*/

DROP TABLE IF EXISTS analytics.orders_clean CASCADE;
CREATE TABLE analytics.orders_clean AS
SELECT
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  DATE(o.order_purchase_timestamp) AS order_date,
  ia.items_count,
  ia.sum_item_price,
  ia.sum_freight_value,
  (ia.sum_item_price + ia.sum_freight_value) AS order_total,
  ia.primary_category,
-- delivery metrics
 CASE 
   WHEN o.order_delivered_customer_date IS NULL OR o.order_purchase_timestamp IS NULL 
   THEN NULL
   ELSE (o.order_delivered_customer_date - o.order_purchase_timestamp)
 END AS days_to_delivery,
 CASE 
   WHEN o.order_delivered_customer_date IS NULL OR o.order_delivered_carrier_date IS NULL 
   THEN NULL
   ELSE (o.order_delivered_customer_date - o.order_delivered_carrier_date)
 END AS delivery_carrier_to_customer_days,
  -- is_delayed flag
  CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN TRUE ELSE FALSE END AS is_delayed,
  -- chronology flag
  CASE WHEN (o.order_purchase_timestamp > o.order_approved_at) OR
            (o.order_approved_at > o.order_delivered_carrier_date) OR
            (o.order_delivered_carrier_date > o.order_delivered_customer_date)
       THEN TRUE ELSE FALSE END AS chronology_flag,
  -- cancel flag
  CASE WHEN o.order_status IN ('canceled','unavailable') THEN TRUE ELSE FALSE END AS is_canceled_or_unavailable,
  -- payments
  ops.sum_payments,
  ops.primary_payment_type,
  ROUND(coalesce(ops.sum_payments,0) / NULLIF((ia.sum_item_price+ia.sum_freight_value),0),4) AS payment_coverage
FROM public.olist_orders o
JOIN (
    SELECT customer_id, customer_unique_id
    FROM public.olist_customers
) c
  ON o.customer_id = c.customer_id
LEFT JOIN analytics.order_items_agg ia USING (order_id)
LEFT JOIN analytics.order_payments_summary ops USING (order_id);

CREATE INDEX ON analytics.orders_clean (order_id);
CREATE INDEX ON analytics.orders_clean (order_date);

/*_____________________________________________________________________________________

   We built a canonical one-row-per-order fact table by joining cleaned dims and aggregates.
   This avoids double counting items/payments.

   We included delivery KPIs, delay/chronology flags, cancel flags, and payment coverage.
   these signals are critical for trust in the data. For example, payment coverage
   shows which orders are underpaid or overpaid, which directly affects financial reporting
   accuracy.
_______________________________________________________________________________________*/



/*
======================================================================
FINAL CONCLUSION & NEXT ACTIONS
-----------------------------------------------------------------------

Summary:
   We created a cleaned analytics layer: dims (geo, customers, products, sellers),
   deduped reviews, item-level flags and aggregates, payment summaries, a canonical
   orders_clean fact table for further analysis and dashboards.

What next:
   Now we have ready-to-analyze data. In our next steps we're going to:
    Basic EDA
    Create RFM Table and Create Segments
    Analyse the Funnel (where we have most drop-offs)
    Geospatial % Ops
   As planned.
======================================================================
*/
