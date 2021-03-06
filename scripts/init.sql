DROP TABLE IF EXISTS schedule;
DROP TABLE IF EXISTS schedule_file;
DROP TABLE IF EXISTS schedule_delete;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS s3data;

CREATE TABLE schedule (
    schedule_id         SERIAL PRIMARY KEY,
    collection_id       varchar(128) NOT NULL,
    collection_path     varchar(128) NOT NULL,
    schedule_time       bigint NOT NULL,
    start_time          bigint,
    complete_time       bigint
);

CREATE TABLE schedule_file (
    schedule_file_id    SERIAL PRIMARY KEY,
    schedule_id         int,
    uri                 varchar(2048) NOT NULL,
    file_location       varchar(2048) NOT NULL,
    complete_time       bigint
);

CREATE TABLE schedule_delete (
    schedule_delete_id  SERIAL PRIMARY KEY,
    schedule_id         int,
    uri                 varchar(2048) NOT NULL,
    complete_time       bigint
);

-- The following table is used to store metadata which contains all uris from
-- the ONS website with links to the content location on the S3 bucket.
--
-- Language of the content is embedded into the uri.
-- EG /about?lang=en, /about?lang=cy
-- This allows the uri column to be unique and support multiple languages.
CREATE TABLE s3data(id SERIAL PRIMARY KEY,
                      collection_id varchar(128) NOT NULL,
                      uri varchar(2048) NOT NULL UNIQUE,
                      s3 varchar(2048) NOT NULL);

CREATE TABLE metadata(id SERIAL PRIMARY KEY,
                      collection_id varchar(128) NOT NULL,
                      uri varchar(2048) NOT NULL UNIQUE,
                      content json NOT NULL);
