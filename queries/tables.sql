CREATE TABLE IF NOT EXISTS customer_data (
    customer_key VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(20) NOT NULL,
    customer_age INTEGER NOT NULL,  
    payment_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS shopping_mall_data (
    mall_name VARCHAR(100) PRIMARY KEY,
    built_year INTEGER,            
    total_area_sqm VARCHAR(50),
    city_location VARCHAR(50),
    total_stores INTEGER           
);

CREATE TABLE IF NOT EXISTS sales_data (
    invoice_number VARCHAR(50) PRIMARY KEY,
    customer_key VARCHAR(50),
    product_category VARCHAR(50),
    quantity_sold INTEGER,         
    invoice_date DATE,
    total_price DECIMAL(10, 2),
    mall_name VARCHAR(100),
    FOREIGN KEY (customer_key) REFERENCES customer_data(customer_key),
    FOREIGN KEY (mall_name) REFERENCES shopping_mall_data(mall_name)
);
