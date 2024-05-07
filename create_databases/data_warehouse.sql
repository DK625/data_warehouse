CREATE TABLE dw.Time_dim (
    time_id SERIAL PRIMARY KEY,
    month_name VARCHAR(255),
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);
CREATE TABLE dw.City_dim (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(255),
    office_addr VARCHAR(255),
    state VARCHAR(255)
);

CREATE TABLE dw.Customer_dim (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(254),
    tour BIT,
    post BIT,
    city_id INTEGER REFERENCES dw.City_dim(city_id)
);
CREATE TABLE dw.Store_dim (
    store_id SERIAL PRIMARY KEY,
    phone_number VARCHAR(20),
    city_id INTEGER REFERENCES dw.City_dim(city_id)
);

CREATE TABLE dw.Product_dim (
    product_id SERIAL PRIMARY KEY,
    description VARCHAR(255),
    size VARCHAR(50),
    weight FLOAT,
    price NUMERIC
);

CREATE TABLE dw.Sale_fact (
    time_id INTEGER REFERENCES dw.Time_dim(time_id),
    city_id INTEGER REFERENCES dw.City_dim(city_id),
    customer_id INTEGER REFERENCES dw.Customer_dim(customer_id),
    product_id INTEGER REFERENCES dw.Product_dim(product_id),
    units_sold INTEGER,
    total_amount_sold NUMERIC
);

CREATE TABLE dw.Inventory_fact (
    time_id INTEGER REFERENCES dw.Time_dim(time_id),
    store_id INTEGER REFERENCES dw.Store_dim(store_id),
    product_id INTEGER REFERENCES dw.Product_dim(product_id),
    quantity INTEGER
);

