--QUERY 1. General query
SELECT *
FROM final_games_dataset;

--Query 2. Monthly Recurring Revenue (MRR)
SELECT 
	payment_month, 
	ROUND( SUM(total_revenue) ) AS mrr
FROM 
	final_games_dataset
GROUP BY 
	payment_month
ORDER BY 
	payment_month;
	
--Query 3. Paid Users
SELECT 
	payment_month, 
	COUNT(DISTINCT user_id) AS paid_users
FROM 
	final_games_dataset
WHERE
	total_revenue > 0
GROUP BY 
	payment_month
ORDER BY
	payment_month;
	
--Query 4. ARPPU
SELECT 
	payment_month, 
	round( SUM(total_revenue) / COUNT(DISTINCT user_id), 2 ) AS arppu
FROM 
	final_games_dataset
WHERE
	total_revenue > 0
GROUP BY 
	payment_month
ORDER BY
	payment_month;
	
--Query 5. New Paid Users
SELECT
	payment_month, 
	COUNT(DISTINCT user_id) AS new_paid_users
FROM 
	final_games_dataset
WHERE 
	status = 'new'
GROUP BY 
	payment_month
ORDER BY 
	payment_month;
	
-- Query 6. New MMR
SELECT
	payment_month, 
	ROUND( SUM(total_revenue)) AS new_mmr
FROM 
	final_games_dataset
WHERE 
	status = 'new'
GROUP BY 
	payment_month
ORDER BY 
	payment_month;
	
-- Query 7. Churn Users
SELECT 
	payment_month,
	COUNT(DISTINCT user_id) AS churned_users
FROM
	final_games_dataset
WHERE
	status = 'churn'
GROUP BY
	payment_month
ORDER BY
	payment_month;

-- Query 8. Churn Rate
WITH paid_users AS (
    SELECT 
    		payment_month, 
    		COUNT(DISTINCT user_id) AS paid_users
    FROM 
    		final_games_dataset
    WHERE 
    		total_revenue > 0
    GROUP BY 
    		payment_month
),
churned_users AS (
    SELECT 
    		payment_month, 
    		COUNT(DISTINCT user_id) AS churned_users
    FROM
    		final_games_dataset
    WHERE
    		status = 'churn'
    GROUP BY
    		payment_month
)
SELECT
	c. payment_month,
	ROUND( CAST(c.churned_users AS REAL) / (p.paid_users), 2 ) AS churn_rate
FROM
	churned_users AS c
LEFT JOIN
	paid_users AS p
ON 
	c.payment_month = date(p.payment_month, "+1 month")
GROUP BY
	c.payment_month
ORDER BY
	c.payment_month;

	
-- Query 9. Churn Revenue
SELECT
	payment_month, 
	ROUND( SUM(total_revenue_previous)) AS churn_revenue
FROM 
	final_games_dataset
WHERE 
	status = 'churn'
GROUP BY 
	payment_month
ORDER BY 
	payment_month;
	
-- Query 10. Revenue Churn Rate
WITH mrr AS (
    SELECT 
    		payment_month,
    		SUM(total_revenue) AS mrr
    FROM
    		final_games_dataset
    GROUP BY
    		payment_month
),
churned_revenue AS (
    SELECT 
    		payment_month, 
    		SUM(total_revenue_previous) AS churned_revenue
    FROM
    		final_games_dataset
    	WHERE
    		status = 'churn'
    GROUP BY 
    		payment_month
)
SELECT
	c.payment_month,
	ROUND((c.churned_revenue / m.mrr), 2 ) AS Revenue_churn_rate
FROM
	churned_revenue AS c
LEFT JOIN
	mrr AS m
ON 
	c.payment_month = date(m.payment_month, '+1 month')
GROUP BY
	c.payment_month
ORDER BY
	c.payment_month;
	
-- Query 11. Expension MRR
SELECT 
	payment_month,
	ROUND(SUM(total_revenue - total_revenue_previous)) AS expansion_mrr
FROM
	final_games_dataset
WHERE
	status = 'active' 
AND
	total_revenue > total_revenue_previous
GROUP BY
	payment_month
ORDER BY
	payment_month;

-- Query 12. Contraction MRR
SELECT
	payment_month,
	ROUND(SUM(total_revenue - total_revenue_previous)) AS contraction_mrr
FROM
	final_games_dataset
WHERE
	status = 'active' 
AND
	total_revenue < total_revenue_previous
GROUP BY
	payment_month
ORDER BY
	payment_month;

