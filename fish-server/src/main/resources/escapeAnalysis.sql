DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS phone;
CREATE TABLE customer (customer_id number(10,0), customer_name varchar(200));
CREATE TABLE phone (customer_id number(10,0), phone_no varchar(200));
