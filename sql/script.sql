--Query 1: Preliminary review and analysis of the data --
SELECT *
FROM games_payments;

SELECT *
FROM games_paid_users;

-- Query 2: Calculating Monthly Recurring Revenue
SELECT
	date_trunc('month', payment_date) AS payment_month,
	sum(revenue_amount_usd) AS mrr
FROM
	games_payments
GROUP BY
	date_trunc('month', payment_date)
ORDER BY
	payment_month;


-- Query 3: Calculating Paid Users
SELECT 
    DATE_TRUNC('month', payment_date) AS payment_month,
    COUNT(DISTINCT user_id) AS paid_users
FROM 
	games_payments
GROUP BY
	DATE_TRUNC('month', payment_date)
ORDER BY 
	payment_month;
	
-- Query 4: Calculating Average Revenue Per Paid User (ARPPU)
WITH mrr AS (
    SELECT 
        DATE_TRUNC('month', payment_date) AS payment_month,
        SUM(revenue_amount_usd) AS mrr
    FROM games_payments
    GROUP BY DATE_TRUNC('month', payment_date)
),
paid_users AS (
    SELECT 
        DATE_TRUNC('month', payment_date) AS payment_month,
        COUNT(DISTINCT user_id) AS paid_users
    FROM games_payments
    GROUP BY DATE_TRUNC('month', payment_date)
)
SELECT 
    mrr.payment_month,
    round(mrr.mrr / NULLIF(paid_users.paid_users, 0), 3) AS arppu
FROM mrr
JOIN paid_users ON mrr.payment_month = paid_users.payment_month
ORDER BY payment_month;

-- Query 5: New Paid Users
WITH first_payments AS (
    SELECT 
        user_id,
        MIN(DATE_TRUNC('month', payment_date)) AS first_payment_month
    FROM games_payments
    GROUP BY user_id
)
SELECT 
    first_payment_month AS payment_month,
    COUNT(DISTINCT user_id) AS new_paid_users
FROM first_payments
GROUP BY first_payment_month
ORDER BY payment_month;


-- Query 6: New MRR
WITH first_payments AS (
    SELECT 
        user_id,
        MIN(DATE_TRUNC('month', payment_date)) AS first_payment_month
    FROM games_payments
    GROUP BY user_id
)
SELECT 
    fp.first_payment_month AS payment_month,
    SUM(gp.revenue_amount_usd) AS new_mrr
FROM first_payments fp
JOIN games_payments gp 
    ON fp.user_id = gp.user_id 
    AND fp.first_payment_month = DATE_TRUNC('month', gp.payment_date)
GROUP BY fp.first_payment_month
ORDER BY payment_month;


--Query 7: Churned Users
WITH monthly_users AS (
    SELECT DISTINCT 
        user_id,
        DATE_TRUNC('month', payment_date) AS payment_month
    FROM games_payments
)
SELECT 
    (mu1.payment_month + interval '1 month') AS payment_month,
    COUNT(DISTINCT mu1.user_id) AS churned_users
FROM monthly_users mu1
LEFT JOIN monthly_users mu2 
    ON mu1.user_id = mu2.user_id 
    AND (mu1.payment_month + interval '1 month') = mu2.payment_month
WHERE mu2.user_id IS NULL
GROUP BY (mu1.payment_month + interval '1 month')
ORDER BY payment_month;


-- Query 8: Churn Rate
WITH paid_users AS (
    SELECT 
        DATE_TRUNC('month', payment_date) AS payment_month,
        COUNT(DISTINCT user_id) AS paid_users
    FROM games_payments
    GROUP BY DATE_TRUNC('month', payment_date)
),
churned AS (
    SELECT 
        (mu1.payment_month + interval '1 month') AS payment_month,
        COUNT(DISTINCT mu1.user_id) AS churned_users
    FROM (SELECT DISTINCT user_id, DATE_TRUNC('month', payment_date) AS payment_month FROM games_payments) mu1
    LEFT JOIN (SELECT DISTINCT user_id, DATE_TRUNC('month', payment_date) AS payment_month FROM games_payments) mu2 
        ON mu1.user_id = mu2.user_id 
        AND (mu1.payment_month + interval '1 month') = mu2.payment_month
    WHERE mu2.user_id IS NULL
    GROUP BY (mu1.payment_month + interval '1 month')
)
SELECT 
    churned.payment_month,
    churned.churned_users::float / NULLIF(LAG(paid_users.paid_users) OVER (ORDER BY churned.payment_month), 0) AS churn_rate
FROM churned
JOIN paid_users ON churned.payment_month = paid_users.payment_month
ORDER BY churned.payment_month;

--Query 9: Churned Revenue
WITH monthly_users AS (
    SELECT DISTINCT 
        user_id,
        DATE_TRUNC('month', payment_date) AS payment_month
    FROM games_payments
),
churned AS (
    SELECT 
        (mu1.payment_month + interval '1 month') AS payment_month,
        mu1.user_id
    FROM monthly_users mu1
    LEFT JOIN monthly_users mu2 
        ON mu1.user_id = mu2.user_id 
        AND (mu1.payment_month + interval '1 month') = mu2.payment_month
    WHERE mu2.user_id IS NULL
)
SELECT 
    churned.payment_month,
    SUM(gp.revenue_amount_usd) AS churned_revenue
FROM churned
JOIN games_payments gp 
    ON churned.user_id = gp.user_id 
    AND DATE_TRUNC('month', gp.payment_date) = (churned.payment_month - interval '1 month')
GROUP BY churned.payment_month
ORDER BY churned.payment_month;


-- Query 10: Revenue Churn Rate
WITH monthly_revenue AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', payment_date) AS payment_month,
        SUM(revenue_amount_usd) AS monthly_revenue
    FROM games_payments
    GROUP BY user_id, DATE_TRUNC('month', payment_date)
),
lagged_revenue AS (
    SELECT 
        user_id,
        payment_month,
        monthly_revenue,
        COALESCE(LAG(monthly_revenue) OVER (PARTITION BY user_id ORDER BY payment_month), 0) AS prev_month_revenue
    FROM monthly_revenue
)
SELECT 
    payment_month,
    COALESCE(SUM(CASE 
        WHEN monthly_revenue > prev_month_revenue 
        THEN monthly_revenue - prev_month_revenue 
        ELSE 0 
    END), 0) AS expansion_mrr
FROM lagged_revenue
GROUP BY payment_month
ORDER BY payment_month;

-- Query 11: Expansion MRR


-- Query 12: Contraction MRR
WITH monthly_revenue AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', payment_date) AS payment_month,
        SUM(revenue_amount_usd) AS monthly_revenue
    FROM games_payments
    GROUP BY user_id, DATE_TRUNC('month', payment_date)
),
lagged_revenue AS (
    SELECT 
        user_id,
        payment_month,
        monthly_revenue,
        COALESCE(LAG(monthly_revenue) OVER (PARTITION BY user_id ORDER BY payment_month), 0) AS prev_month_revenue
    FROM monthly_revenue
)
SELECT 
    payment_month,
    COALESCE(SUM(CASE 
        WHEN monthly_revenue < prev_month_revenue 
        THEN prev_month_revenue - monthly_revenue 
        ELSE 0 
    END), 0) AS contraction_mrr
FROM lagged_revenue
GROUP BY payment_month
ORDER BY payment_month;

-- Query 13: Customer LifeTime (LT)

-- Query 14: Customer LifeTime Value (LTV)
