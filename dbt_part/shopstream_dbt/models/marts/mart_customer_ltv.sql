-- Mart: Customer Lifetime Value & RFM Segmentation
-- Layer: marts
-- Grain: one row per customer (with completed orders)
-- Use case: marketing segmentation, churn risk scoring.
-- RFM cutoffs (30/90/180 days) are conventional defaults; tune per business.

{{ config(
    materialized='table',
    tags=['mart', 'customer']
) }}

with dim_customers as (

    select * from {{ ref('dim_customers') }}

),

fact_orders as (

    select * from {{ ref('fact_orders') }}
    where is_completed = 1

),

customer_metrics as (

    select
        f.customer_key,

        -- Recency
        max(f.order_timestamp)                                              as last_order_date,
        datediff(day, max(f.order_timestamp), current_date())               as days_since_last_order,

        -- Frequency
        count(distinct f.order_key)                                         as total_orders,
        datediff(day, min(f.order_timestamp), max(f.order_timestamp))       as customer_lifespan_days,

        -- Monetary
        sum(f.line_revenue)                                                 as lifetime_value,
        avg(f.order_total)                                                  as avg_order_value,

        -- bookkeeping
        min(f.order_timestamp)                                              as first_order_date

    from fact_orders f
    group by 1

),

rfm_scores as (

    select
        *,
        -- Each score 1..5; 5 = best (most recent / most frequent / highest spend).
        ntile(5) over (order by days_since_last_order asc)  as recency_score,
        ntile(5) over (order by total_orders          desc) as frequency_score,
        ntile(5) over (order by lifetime_value        desc) as monetary_score
    from customer_metrics

),

rfm_segments as (

    select
        *,
        recency_score + frequency_score + monetary_score                    as rfm_total_score,

        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3                         then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2                         then 'Promising'
            when recency_score >= 3 and monetary_score  >= 3                         then 'Potential Loyalists'
            when recency_score <= 2 and frequency_score >= 3                         then 'At Risk'
            when recency_score <= 2 and monetary_score  >= 4                         then 'Cant Lose Them'
            when recency_score <= 2                                                  then 'Hibernating'
            else 'Others'
        end                                                                 as rfm_segment

    from rfm_scores

),

final as (

    select
        c.customer_key,
        c.customer_id,
        c.email,
        c.full_name,
        c.country_code,
        c.customer_segment,
        c.customer_status,

        -- RFM metrics
        r.last_order_date,
        r.days_since_last_order,
        r.total_orders,
        r.customer_lifespan_days,
        r.lifetime_value,
        r.avg_order_value,
        r.first_order_date,

        -- scores & segment
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total_score,
        r.rfm_segment,

        -- churn risk bucketing (30/90/180-day windows; see ADR notes)
        case
            when r.days_since_last_order > 180 then 'High'
            when r.days_since_last_order > 90  then 'Medium'
            else 'Low'
        end                                                                 as churn_risk

    from dim_customers c
    inner join rfm_segments r on c.customer_key = r.customer_key

)

select * from final
