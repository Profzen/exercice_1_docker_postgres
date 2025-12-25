CREATE OR REPLACE VIEW accidents_monthly_stats AS
SELECT
  EXTRACT(YEAR FROM date_accident)::int AS year,
  EXTRACT(MONTH FROM date_accident)::int AS month,
  COUNT(*) AS total_accidents,
  ROUND(AVG(CASE WHEN alcool = 'Positif' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS taux_alcool_positif
FROM accidents_clean
GROUP BY 1, 2
ORDER BY 1, 2;
