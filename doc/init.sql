CREATE TABLE schedule ( schedule_id SERIAL PRIMARY KEY,
    collection_id varchar(50) NOT NULL,
    encryption_key varchar(50) NOT NULL,
    schedule_time bigint NOT NULL,
    start_time bigint,
    complete_time bigint
);
