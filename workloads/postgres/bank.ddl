-- file: bank.ddl

CREATE TABLE ref_data (
    acc_no BIGINT PRIMARY KEY,
    external_ref_id UUID,
    created_time TIMESTAMPTZ,
    acc_details VARCHAR
);

CREATE TABLE orders (
    acc_no BIGINT NOT NULL,
    id UUID NOT NULL default gen_random_uuid(),
    status VARCHAR NOT NULL,
    amount DECIMAL(15, 2),
    ts TIMESTAMPTZ default now(),
    CONSTRAINT pk PRIMARY KEY (acc_no, id)
);
