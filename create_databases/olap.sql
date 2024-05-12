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


CREATE TABLE olap.store_product_info(
	store_id INTEGER REFERENCES olap.store_dim(store_id),
    city_name VARCHAR(255),
	office_addr VARCHAR(255),
	phone_number VARCHAR(20),
	product_id INTEGER REFERENCES olap.product_dim(product_id),
	description VARCHAR(255),
	size VARCHAR(255),
	weight FLOAT,
	price NUMERIC
)
INSERT INTO olap.store_product_info(store_id, city_name, office_addr, phone_number, product_id, description, size, weight, price)
SELECT s.store_id, c.city_name, c.office_addr, s.phone_number, p.product_id, p.description, p.size, p.weight, p.price
FROM dw.store_dim s
JOIN dw.city_dim c ON s.city_id = c.city_id
JOIN dw.sale_fact sf ON sf.city_id = s.city_id
JOIN dw.inventory_fact if ON if.store_id = s.store_id
JOIN dw.product_dim p ON p.product_id = sf.product_id
JOIN dw.product_dim pif ON pif.product_id = if.product_id
ORDER BY s.store_id ASC;






CREATE TABLE olap.all_order (
    customer_id INTEGER REFERENCES olap.Customer_dim(customer_id),
--	city_id INTEGER REFERENCES olap.City_dim(city_id),
    product_id INTEGER REFERENCES olap.Product_dim(product_id),
    units_sold INTEGER,
    total_amount_sold NUMERIC,
	time_id INTEGER REFERENCES olap.Time_dim(time_id),
--	year INTEGER,
--	quarter INTEGER,
--	month INTEGER
);

insert into olap.all_order
SELECT sf.customer_id, sf.city_id, sf.product_id, sf.units_sold, sf.total_amount_sold, sf.time_id, td.year, td.quarter, td.month
FROM dw.sale_fact sf
join olap.time_dim td on td.time_id = sf.time_id
order by customer_id asc