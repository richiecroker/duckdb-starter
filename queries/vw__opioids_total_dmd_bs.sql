WITH max_month AS (
  SELECT MAX(month) AS max_ts
  FROM measures.vw__opioids_total_dmd_bs
)
SELECT *
FROM measures.vw__opioids_total_dmd_bs
CROSS JOIN max_month
WHERE DATE(month) BETWEEN DATE_SUB(DATE(max_ts), INTERVAL 2 MONTH) AND DATE(max_ts)