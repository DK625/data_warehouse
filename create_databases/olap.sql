CREATE TABLE olap.quantity_sale_year_fact(
	year INTEGER,
	units_sold INTEGER,
	total_amount_sold NUMERIC
)
INSERT INTO olap.quantity_sale_year_fact
SELECT t.year, SUM(s.units_sold) ,SUM(s.total_amount_sold)
FROM olap.time_dim t
INNER JOIN dw.sale_fact s
ON t.time_id = s.time_id
GROUP BY t.year
ORDER BY t.year ASC


CREATE TABLE olap.quantity_sale_quarter_fact(
	quarter INTEGER,
	year INTEGER,
	units_sold INTEGER,
	total_amount_sold NUMERIC
)
INSERT INTO olap.quantity_sale_quarter_fact
SELECT t.quarter, t.year, SUM(s.units_sold), SUM(s.total_amount_sold)
FROM olap.time_dim t
INNER JOIN dw.sale_fact s
ON t.time_id = s.time_id
GROUP BY t.quarter, t.year
ORDER BY t.quarter ASC, t.year ASC

CREATE TABLE olap.Time_dim (
    time_id SERIAL PRIMARY KEY,
    month_name VARCHAR(255),
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);
