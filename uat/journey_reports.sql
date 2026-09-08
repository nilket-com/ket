-- PRODUCT-0161: provision only at the explicitly configured reporting destination.
CREATE TABLE IF NOT EXISTS journey_reports (
 report_id VARCHAR NOT NULL PRIMARY KEY,
 install_id VARCHAR NOT NULL,
 run_id VARCHAR NOT NULL,
 sequence BIGINT NOT NULL,
 journey_id VARCHAR,
 step VARCHAR NOT NULL,
 outcome VARCHAR NOT NULL,
 reason VARCHAR,
 dropped BIGINT NOT NULL,
 platform VARCHAR NOT NULL,
 app_version VARCHAR NOT NULL,
 at_ms BIGINT NOT NULL
);
