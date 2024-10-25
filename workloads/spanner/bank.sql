CREATE TABLE ref_data (
    acc_no INT64 ,
    external_ref_id BYTES(16),
    created_time TIMESTAMP,
    acc_details STRING(1024),
)
primary key (acc_no);

CREATE TABLE orders (
    acc_no INT64 NOT NULL,
    id BYTES(16) NOT NULL,
    status STRING(1024) NOT NULL,
    amount FLOAT64,
    ts TIMESTAMP
) PRIMARY KEY (acc_no, id);
