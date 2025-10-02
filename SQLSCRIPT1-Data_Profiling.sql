/*
==========================================
# TOTAL NO. OF ROWS FROM EACH TABLE
==========================================
*/
SELECT 'olist_customers' AS table_name, COUNT(*) AS row_count FROM olist_customers
UNION ALL
SELECT 'olist_geolocation', COUNT(*) FROM olist_geolocation
UNION ALL
SELECT 'olist_order_items', COUNT(*) FROM olist_order_items
UNION ALL
SELECT 'olist_order_payments', COUNT(*) FROM olist_order_payments
UNION ALL
SELECT 'olist_order_reviews', COUNT(*) FROM olist_order_reviews
UNION ALL
SELECT 'olist_orders', COUNT(*) FROM olist_orders
UNION ALL
SELECT 'olist_products', COUNT(*) FROM olist_products
UNION ALL
SELECT 'olist_sellers', COUNT(*) FROM olist_sellers
UNION ALL
SELECT 'product_category_name_translation', COUNT(*) FROM product_category_name_translation;

/*____________________________________________________________________________________
	
   Understanding the Row Count:
   The row counts are the same in customers and orders table, but it's not a 1:1 relationship.
   The number of unique customers is lower than the number of orders, which means some customers placed more than one order.
   It's normal for the number of orders to be slightly higher than the number of reviews since not 
   all customers leave a review.
   There are more payments than orders because a single order can have multiple payment transactions 
   (e.g., split payments or installments).

_______________________________________________________________________________________*/

/*
=================================================
UNIQUE KEY CHECK
-------------------------------------------------
Verifying that the primary keys in customers, orders, and products are truly unique.
This is crucial for accurate joins and aggregations later on.
=================================================
*/
SELECT 'customer_id' AS column_name,
       COUNT(*) AS total_count, 
       COUNT(DISTINCT customer_id) AS unique_id_count,
       COUNT(customer_id) - COUNT(DISTINCT(customer_id)) AS duplicates
FROM olist_customers
UNION ALL
SELECT 'customer_unique_id' AS column_name,
       COUNT(*) AS total_count,
       COUNT(DISTINCT customer_unique_id) AS unique_id_count,
       COUNT(customer_unique_id) - COUNT(DISTINCT(customer_unique_id)) AS duplicates
FROM olist_customers
UNION ALL
SELECT 'order_id' AS column_name,
       COUNT(*) AS total_count, 
       COUNT(DISTINCT order_id) AS unique_id_count,
       COUNT(order_id) - COUNT(DISTINCT(order_id)) AS duplicates
FROM olist_orders
UNION ALL
SELECT 'order_item_id' AS column_name,
       COUNT(*) AS total_count, 
       COUNT(DISTINCT order_item_id) AS unique_id_count,
       COUNT(order_item_id) - COUNT(DISTINCT(order_item_id)) AS duplicates
FROM olist_order_items
UNION ALL
SELECT 
       'product_id' AS column_name,
       COUNT(*) AS total_count, 
       COUNT(DISTINCT product_id) AS unique_id_count,
       COUNT(product_id) - COUNT(DISTINCT(product_id)) AS duplicates
FROM olist_products
UNION ALL
SELECT 
       'seller_id' AS column_name,
       COUNT(*) AS total_count, 
       COUNT(DISTINCT seller_id) AS unique_id_count,
       COUNT(seller_id) - COUNT(DISTINCT(seller_id)) AS duplicates
FROM olist_sellers
UNION ALL
SELECT 
       'review_id' AS column_name,
       COUNT(*) AS total_count, 
       COUNT(DISTINCT review_id) AS unique_id_count,
       COUNT(review_id) - COUNT(DISTINCT(review_id)) AS duplicates
FROM olist_order_reviews;


/*_____________________________________________________________________

   `There are 814 duplicate entries in review_id which can be a data quality issue and
   it will require investigation to fix this. The duplicates in customer_unique_id and
   order_item_id were expected. customer_unique_id tracks individual customers who can
   place multiple orders, and order_item_id simply denotes an item's position within a
   specific order. All other primary keys [customer_id, order_id, product_id, seller_id] 
   had zero duplicates, which is a good result.
_____________________________________________________________________*/


/*
==================================================
# CHECKING REFRAL INTEGRATIY
--------------------------------------------------
This involves checking if foreign keys in one table exist as primary keys in
the table they reference.
==================================================
*/

SELECT 'orders_to_customers' AS relationship, COUNT(*) AS orphan_records
FROM olist_orders o
LEFT JOIN olist_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'order_items_to_orders' AS relationship, COUNT(*) AS orphan_records
FROM olist_order_items oi
LEFT JOIN olist_orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
UNION ALL
SELECT 'order_items_to_products' AS relationship, COUNT(*) AS orphan_records
FROM olist_order_items oi
LEFT JOIN olist_products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'order_items_to_sellers' AS relationship, COUNT(*) AS orphan_records
FROM olist_order_items oi
LEFT JOIN olist_sellers s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL
UNION ALL
SELECT 'reviews_to_orders' AS relationship, COUNT(*) AS orphan_records
FROM olist_orders o
LEFT JOIN olist_order_reviews r ON r.order_id = o.order_id
WHERE o.order_id IS NULL; 
/*__________________________________________________________________

   There are no orphaned records in our data. All foreign keys in one table
   exist as primary keys in the other table as reference.
____________________________________________________________________*/

-- --------------------------------------------------------------------------
-- Phase-B. Column By Column analysis for invalid or unknown values.
-- --------------------------------------------------------------------------

/*
========================================
b.1 (categorical columns)
--------------------------------------------
This query involes checking if we have any invalid, mispelled or unknown values in
our categorical columns.
========================================
*/

-- categories in order_status
SELECT order_status, COUNT(*) AS count
FROM olist_orders
GROUP BY order_status;

-- categories in payment_type
SELECT payment_type, COUNT(*) AS count
FROM olist_order_payments
GROUP BY payment_type;

-- catgories in product
SELECT product_category_name, COUNT(*) AS count
FROM olist_products
GROUP BY product_category_name
ORDER BY count DESC; -- we got 70+ categories

-- Gettings categories which doesn't have any translation
SELECT p.product_category_name, COUNT(p.product_category_name) AS total_count
FROM olist_products p
LEFT JOIN product_category_name_translation t
ON p.product_category_name = t.product_category_name
WHERE t.product_category_name IS NULL
  AND p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY total_count DESC;

/*_____________________________________________________________
 
   We have no unknown categories in order_status but we have:
   7 orders which were created/approved but not delivered.
   'not_defined' category in payment_type but it only appeared 3 times. 
   2 categories which don't appear in product_category_translation,
   and we have some nulls in the column.
_______________________________________________________________*/      

/*
=====================================================
 b.2 (numeric columns)
-----------------------------------------------------
This involves checking if any Columns like (price, freight_value, review_score, etc)
have any negative, zero or higher than expected (oulier) values.
====================================================
*/

SELECT 'price' AS column_name,
       'olist_order_items' AS table_name,
        COUNT(*) AS invalid_values_count
FROM olist_order_items
WHERE price <= 0

UNION ALL

SELECT 'freight_value' AS column_name,
       'olist_order_items' AS table_name,
        COUNT(*) AS invalid_values_count
FROM olist_order_items
WHERE freight_value <= 0

UNION ALL

SELECT 'review_score' AS column_name,
       'olist_order_reviews' AS table_name,
        COUNT(*) AS invalid_values_count
FROM olist_order_reviews
WHERE review_score NOT BETWEEN 1 AND 5

UNION ALL

SELECT 'product_name_lenght' AS column_name,
       'olist_products' AS table_name,
        COUNT(*) AS invalid_values_count
FROM olist_products
WHERE product_name_lenght <= 3

UNION ALL

SELECT 'photos_qty' AS column_name,
       'olist_products' AS table_name,
        COUNT(*) AS invalid_values_count
FROM olist_products
WHERE product_photos_qty <= 0

UNION ALL

SELECT 'product_weight_g' AS column_name,
       'olist_products' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_products
WHERE product_weight_g <= 0

UNION ALL

SELECT 'product_length' AS column_name,
       'olist_products' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_products
WHERE product_length <= 0

UNION ALL

SELECT 'product_height_cm' AS column_name,
       'olist_products' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_products
WHERE product_height_cm <= 0

UNION ALL

SELECT 'product_width_cm' AS column_name,
       'olist_products' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_products
WHERE product_width_cm <= 0

UNION ALL

SELECT 'payment_sequential' AS column_name,
       'olist_order_payments' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_order_payments
WHERE payment_sequential <= 0

UNION ALL

SELECT 'payment_installments' AS column_name,
       'olist_order_payments' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_order_payments
WHERE payment_installments < 0

UNION ALL

SELECT 'payment_value' AS column_name,
       'olist_order_payments' AS table_name,
       COUNT(*) AS invalid_values_count
FROM olist_order_payments
WHERE payment_value <= 0;

/*____________________________________________________________
 
  We got invalid values in 3 columns which can be a data quality issue
  freight_value : 383 , payment_value : 9 and product_weight_g : 4
______________________________________________________________*/

/*
=======================================================
 b.4 (geo columns)
=======================================================
*/

-- Invalid latitude
SELECT 'latitude' AS column,
        COUNT(*) AS invalid_count
FROM olist_geolocation
WHERE geolocation_lat NOT BETWEEN -90 AND 90 
UNION ALL
-- Invalid longitude
SELECT 'longitude' AS column,
        COUNT(*) AS invalid_count
FROM olist_geolocation
WHERE geolocation_lng NOT BETWEEN -180 AND 180;

-- Same city with case/accents variation
CREATE EXTENSION IF NOT EXISTS unaccent; -- adding unaccent extension so we can get city names which are same but getting counted as different value
SELECT LOWER(TRIM(unaccent(geolocation_city))) AS city_norm,
       COUNT(DISTINCT geolocation_city) AS variants,
       ARRAY_AGG(DISTINCT geolocation_city) AS raw_variants,
       COUNT(*) AS total_rows
FROM olist_geolocation
GROUP BY LOWER(TRIM(unaccent(geolocation_city)))
HAVING COUNT(DISTINCT geolocation_city) > 1
ORDER BY total_rows DESC;

-- Zip prefix mapping to more than one state
SELECT geolocation_zip_code_prefix,
       array_agg(DISTINCT geolocation_state ORDER BY geolocation_state) AS states,
       COUNT(DISTINCT geolocation_state) AS state_count,
       array_agg(DISTINCT geolocation_city ORDER BY geolocation_city) AS cities
FROM olist_geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(DISTINCT geolocation_state) > 1
ORDER BY state_count DESC, geolocation_zip_code_prefix;

/*______________________________________________________________

    Latitudes and Longitudes are valid.  
    We found 8 zip code prefixes mapped to more than 1 state.  
    Some are due to accents (e.g., São Paulo), but others 
    are real overlaps where the same prefix appears in different states (like AC/RJ, DF/GO, etc.).  
    In city names, there are many duplicates caused by case/accents
    (e.g., 'sao paulo' vs 'são paulo', 'brasilia' vs 'brasília').
________________________________________________________________*/


/*
======================================================
 b.5 (DATE COLUMNS)
======================================================
*/
-- Checking for chronological violation
SELECT order_id, order_purchase_timestamp, order_approved_at,
       order_delivered_carrier_date, order_delivered_customer_date
FROM olist_orders
WHERE order_purchase_timestamp > order_approved_at
   OR order_approved_at > order_delivered_carrier_date
   OR order_delivered_carrier_date > order_delivered_customer_date;

-- orders which are delivered before and after estimated dates
SELECT COUNT(*) AS delayed_orders
FROM olist_orders
WHERE order_delivered_customer_date > order_estimated_delivery_date;

SELECT COUNT(*) AS orders_before_estimated_date
FROM olist_orders
WHERE order_delivered_customer_date <= order_estimated_delivery_date;

-- anomalies in order items table if any shipping limit date is greater than purchase date
SELECT oi.order_id, oi.shipping_limit_date, o.order_purchase_timestamp
FROM olist_order_items oi
JOIN olist_orders o ON oi.order_id = o.order_id
WHERE oi.shipping_limit_date < o.order_purchase_timestamp;

-- anomalies in review table if any review answer date is greater than review creation date
SELECT review_id, order_id, review_creation_date, review_answer_timestamp
FROM olist_order_reviews
WHERE review_answer_timestamp < review_creation_date;

/*_____________________________________________________________________________

   We have 6535 delayed orders and 700 dates that aren't in chronological order. 
   Shipping limit dates or review answer dates have 0 invalid entries, which is good.
________________________________________________________________________________*/


/*
=====================================================
 b.6 (TEXT COLUMNS)
=====================================================
*/
SELECT review_id, review_comment_message
FROM olist_order_reviews
WHERE TRIM(review_comment_message) = ''
   OR LENGTH(review_comment_message) < 5
   OR review_comment_message IS NULL;

/*_________________________________________________________
    
    About 59k of the reviews are either very short or null. 
    Also, there are a lot of spam reviews with phrases like "ok,"
    "bom," "...", etc. So, about 60% of review data is useless
    for natural language processing.
___________________________________________________________*/



/*
==============================================================
 Checking Nulls and null % for each columns
--------------------------------------------------------------
 We are using Dynamic SQL here as doing it using static SQL
 will take too much time and code will be redundent
==============================================================
*/

-- Create temp table
DROP TABLE IF EXISTS temp_null_table;
CREATE TEMP TABLE temp_null_table (
    table_name TEXT,
    column_name TEXT,
    null_count BIGINT,
    total_rows BIGINT,
    null_percent NUMERIC(6,2)
);

-- Populate dynamically
DO $$
DECLARE
    rec RECORD;
    result RECORD;
    col_sql TEXT;
    total_sql TEXT;
    total_rows BIGINT;
BEGIN
    FOR rec IN
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
              'olist_customers', 'olist_orders', 'olist_order_items',
              'olist_order_payments', 'olist_order_reviews',
              'olist_products', 'product_category_name_translation',
              'olist_sellers', 'olist_geolocation'
          )
    LOOP
        -- Get total row count
        total_sql := format('SELECT COUNT(*) FROM %I.%I', rec.table_schema, rec.table_name);
        EXECUTE total_sql INTO total_rows;

        -- Build and run column null count
        col_sql := format(
            'SELECT COUNT(*) FILTER (WHERE %I IS NULL) FROM %I.%I',
            rec.column_name, rec.table_schema, rec.table_name
        );
        EXECUTE col_sql INTO result;

        -- Insert result only if nulls found
        IF result.count > 0 THEN
            INSERT INTO temp_null_table (
                table_name,
                column_name,
                null_count,
                total_rows,
                null_percent
            ) VALUES (
                rec.table_name,
                rec.column_name,
                result.count,
                total_rows,
                ROUND((result.count::DECIMAL / GREATEST(total_rows, 1)) * 100, 2)
            );
        END IF;
    END LOOP;
END $$;

-- Query result
SELECT *
FROM temp_null_table
ORDER BY null_percent DESC;


/*________________________________________________________________________________________________
   
   The reviews table contains most of the null values, as expected [about 88% in comment_tittle and about 58% in comment_message].
   Additionally, there are some nulls in the product table (a data quality issue) and in order table
   (probably because the order was canceled or not delivered). 
   We need to investigate and flag/fill null values or we'll exclude rows which are not usefull for our analysis.

____________________________________________________________________________________________________*/


/*=====================================================================
       # Final Conclusion
-----------------------------------------------------------------------
    From start to end we understood/explored that:
- What different and same total no. of rows means here?
- Do our unique keys contain duplicates? If yes, then what does it mean?
- Are our primary keys valid as foreign keys? 
- Do we have any invalid values in any columns?
- How many null values does our data have and in which columns?
----------------------------------------------------------------------
 What next:
    So, now we know everything we need to know about data before starting to clean it.
    In our next step we are going to handle these anomalies we found by flagging them
    and prepare clean analytics layer, dim tables and make fact table  "orders_clean"
    for further analysis.
======================================================================*/

