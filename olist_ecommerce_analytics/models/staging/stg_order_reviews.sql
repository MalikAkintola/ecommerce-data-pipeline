with raw_order_reviews as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    from {{ source('raw', 'olist_order_reviews') }}
)
select
    review_id,
    order_id,
    review_score,
    coalesce(trim(review_comment_title), 'No Title')   as review_comment_title,
    coalesce(trim(review_comment_message), 'No Comment') as review_comment_message,
    review_creation_date         as review_created_date,
    review_answer_timestamp      as review_answered_timestamp
from raw_order_reviews