-- file: bank.sql

-- read operation, executed 50% of the time
SELECT * FROM orders WHERE acc_no = %s AND id = %s;

-- below 2 transactions constitute a complete order execution

-- new_order
INSERT INTO orders (acc_no, status, amount) VALUES (%s, 'Pending', %s) RETURNING id;

-- execute order - this is an explicit transaction
SELECT * FROM ref_data WHERE acc_no = %s;
UPDATE orders SET status = 'Complete' WHERE (acc_no, id) = (%s, %s);
