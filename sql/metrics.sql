-- Monthly revenue & orders
SELECT
  date_trunc('month', order_date::timestamp)::date AS month,
  COUNT(*) AS orders,
  SUM(order_total) AS revenue,
  ROUND((SUM(order_total) / NULLIF(COUNT(*), 0))::numeric, 2) AS aov
FROM analytics.fct_orders
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- Top customers by lifetime revenue
SELECT
  c.customer_id,
  c.full_name,
  ROUND(SUM(o.order_total)::numeric, 2) AS lifetime_revenue,
  COUNT(*) AS orders
FROM analytics.fct_orders o
JOIN analytics.dim_customers c USING (customer_id)
GROUP BY 1,2
ORDER BY lifetime_revenue DESC
LIMIT 10;
