1) Create the session-level table ga4_stg01_sessions.sqlx

config {
  type: "table",
  schema: "source_tables",
  name: "ga4_stg01_sessions"
}
WWITH session_data AS (
  SELECT
    event_bundle_sequence_id AS session_id,
    user_pseudo_id,
    device.category AS device,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS date,
    MIN(event_timestamp) AS session_start_timestamp,
    MAX(CASE WHEN param.key = 'source' THEN param.value.string_value END) AS source,
    MAX(CASE WHEN param.key = 'medium' THEN param.value.string_value END) AS medium,
    MAX(CASE WHEN param.key = 'campaign' THEN param.value.string_value END) AS campaign,
    MAX(CASE WHEN event_name = 'page_view' AND param.key = 'page_location' THEN param.value.string_value END) AS landing_page,
    LAST_VALUE(CASE WHEN event_name = 'page_view' AND param.key = 'page_location' THEN param.value.string_value IGNORE NULLS) OVER (PARTITION BY event_bundle_sequence_id ORDER BY event_timestamp) AS exit_page,
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_duration_in_sec,
    CASE
      WHEN MAX(CASE WHEN event_name = 'purchase' THEN TRUE ELSE FALSE END) = TRUE THEN TRUE
      WHEN COUNTIF(event_name = 'view_item') >= 2 THEN TRUE
      WHEN (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 >= 10 THEN TRUE
      ELSE FALSE
    END AS is_session_engaged
  FROM
    `source_tables_USER_INDEX.source_ga4_events`, UNNEST(event_params) AS param
  GROUP BY
    event_bundle_sequence_id, user_pseudo_id, device.category, DATE(TIMESTAMP_MICROS(event_timestamp))
)

SELECT
  session_id,
  user_pseudo_id,
  device,
  date,
  session_start_timestamp,
  source,
  medium,
  campaign,
  landing_page,
  exit_page,
  session_duration_in_sec,
  is_session_engaged
FROM
  session_data;



2) Fix the error in the shopify_order_stg01.sqlx file.

config {
  type: "table",
  schema: "source_tables_USER_INDEX",
  name: "shopify_order_stg01"
}

SELECT
  order_id,
  MAX(customer_id) AS customer_id,
  MAX(order_date) AS order_date,
  MAX(order_status) AS order_status,
  SUM(order_value) AS order_value,
  MAX(payment_method) AS payment_method
FROM
  `source_tables_USER_INDEX.shopify_raw_orders`
GROUP BY
  order_id

3) To add the Shopify product collection names to the source_shopify_orderlines_stg01 table, joining the collections data.

config {
  type: "table",
  schema: "source_tables_USER_INDEX",
  name: "shopify_orderlines_stg02_with_collection"
}

WITH collection_data AS (
  SELECT
    collects.product_id,
    STRING_AGG(DISTINCT smart_collections.title, ', ') AS collection_names
  FROM
    `source_tables_USER_INDEX.source_shopify_collects` AS collects
  LEFT JOIN
    `source_tables_USER_INDEX.source_shopify_smart_collections` AS smart_collections
    ON collects.collection_id = smart_collections.id
  LEFT JOIN
    `source_tables_USER_INDEX.source_shopify_custom_collections` AS custom_collections
    ON collects.collection_id = custom_collections.id
  GROUP BY
    collects.product_id
),

orderlines_with_collections AS (
  SELECT
    orderlines.*,
    collection_data.collection_names
  FROM
    `source_tables_USER_INDEX.source_shopify_orderlines_stg01` AS orderlines
  LEFT JOIN
    collection_data
    ON orderlines.product_id = collection_data.product_id
)

SELECT
  order_id,
  order_line_id,
  product_id,
  product_name,
  quantity,
  price,
  IFNULL(collection_names, 'No Collections') AS collection_names
FROM
  orderlines_with_collections;



