
USE SCHEMA RAW;


CREATE OR REPLACE TABLE raw.olist_customers (
    customer_id                 STRING,
    customer_unique_id          STRING,
    customer_zip_code_prefix    STRING,
    customer_city               STRING,
    customer_state              STRING
);


CREATE OR REPLACE TABLE raw.olist_orders (
    order_id                        STRING,
    customer_id                     STRING,
    order_status                    STRING,
    order_purchase_timestamp        TIMESTAMP_NTZ,
    order_approved_at               TIMESTAMP_NTZ,
    order_delivered_carrier_date    TIMESTAMP_NTZ,
    order_delivered_customer_date   TIMESTAMP_NTZ,
    order_estimated_delivery_date   TIMESTAMP_NTZ
);


CREATE OR REPLACE TABLE raw.olist_order_items (
    order_id                STRING,
    order_item_id           INT,
    product_id              STRING,
    seller_id               STRING,
    shipping_limit_date     TIMESTAMP_NTZ,
    price                   NUMBER(10,2),
    freight_value           NUMBER(10,2)
);


CREATE OR REPLACE TABLE raw.olist_order_payments (
    order_id                STRING,
    payment_sequential      INT,
    payment_type            STRING,
    payment_installments    INT,
    payment_value           NUMBER(10,2)
);


CREATE OR REPLACE TABLE raw.olist_order_reviews (
    review_id                   STRING,
    order_id                    STRING,
    review_score                INT,
    review_comment_title        STRING,
    review_comment_message      STRING,
    review_creation_date        TIMESTAMP_NTZ,
    review_answer_timestamp     TIMESTAMP_NTZ
);


CREATE OR REPLACE TABLE raw.olist_products (
    product_id                      STRING,
    product_category_name           STRING,
    product_name_length             INT,
    product_description_length      INT,
    product_photos_qty              INT,
    product_weight_g                NUMBER(10,2),
    product_length_cm               NUMBER(10,2),
    product_height_cm               NUMBER(10,2),
    product_width_cm                NUMBER(10,2)
);


CREATE OR REPLACE TABLE raw.olist_sellers (
    seller_id               STRING,
    seller_zip_code_prefix  STRING,
    seller_city             STRING,
    seller_state            STRING
);


CREATE OR REPLACE TABLE raw.olist_product_category_name_translation (
    product_category_name           STRING,
    product_category_name_english   STRING
);


CREATE OR REPLACE TABLE raw.olist_geolocation (
    geolocation_zip_code_prefix     STRING,
    geolocation_lat                 FLOAT,
    geolocation_lng                 FLOAT,
    geolocation_city                STRING,
    geolocation_state               STRING
);


CREATE OR REPLACE FILE FORMAT raw.csv_format
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


CREATE OR REPLACE STAGE raw.olist_stage
URL = 's3://olistdata-malikola/raw/olist'
CREDENTIALS=(AWS_KEY_ID='AWS_KEY_ID' AWS_SECRET_KEY='AWS_SECRET_KEY')
FILE_FORMAT = raw.csv_format;



--Copy data from S3 stage to Snowflake tables
COPY INTO raw.olist_customers
FROM @raw.olist_stage/olist_customers_dataset.csv;

COPY INTO raw.olist_orders
FROM @raw.olist_stage/olist_orders_dataset.csv;

COPY INTO raw.olist_order_items
FROM @raw.olist_stage/olist_order_items_dataset.csv;

COPY INTO raw.olist_order_payments
FROM @raw.olist_stage/olist_order_payments_dataset.csv;

COPY INTO raw.olist_order_reviews
FROM @raw.olist_stage/olist_order_reviews_dataset.csv;

COPY INTO raw.olist_products
FROM @raw.olist_stage/olist_products_dataset.csv;

COPY INTO raw.olist_sellers
FROM @raw.olist_stage/olist_sellers_dataset.csv;

COPY INTO raw.olist_geolocation
FROM @raw.olist_stage/olist_geolocation_dataset.csv;

COPY INTO raw.olist_product_category_name_translation
FROM @raw.olist_stage/product_category_name_translation.csv;

