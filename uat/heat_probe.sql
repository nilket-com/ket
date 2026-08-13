-- Purpose-built cells for the PRODUCT-0092 heatmap exhibit.
-- Idempotent: rerunning against the local Toron endpoint replaces both tables.
--
-- heat_probe is already aggregated to one row per observed (region, quarter)
-- cell. South/Q2 is deliberately absent, and the signed values exercise the
-- two-ended continuous domain.
--
-- heat_probe_raw carries the same observations before aggregation. North/Q1
-- appears twice, so authoring it directly must refuse rather than silently
-- selecting or aggregating a value.
DROP TABLE IF EXISTS heat_probe;
DROP TABLE IF EXISTS heat_probe_raw;

CREATE TABLE heat_probe (
	region STRING NOT NULL,
	quarter STRING NOT NULL,
	margin FLOAT64 NOT NULL
);

INSERT INTO heat_probe VALUES
('East',  'Q1',  -5.0),
('East',  'Q2',  20.0),
('North', 'Q1', -30.0),
('North', 'Q2',  10.0),
('South', 'Q1',  40.0);

CREATE TABLE heat_probe_raw (
	region STRING NOT NULL,
	quarter STRING NOT NULL,
	margin FLOAT64 NOT NULL
);

INSERT INTO heat_probe_raw VALUES
('East',  'Q1',  -5.0),
('East',  'Q2',  20.0),
('North', 'Q1', -10.0),
('North', 'Q1', -20.0),
('North', 'Q2',  10.0),
('South', 'Q1',  40.0);
