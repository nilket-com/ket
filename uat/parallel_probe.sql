-- PRODUCT-0105 parallel surface exhibit data.
-- Eight quantitative dimensions prove projection is no longer capped by the
-- seven-field array inherited from PRODUCT-0094.
DROP TABLE IF EXISTS parallel_probe;

CREATE TABLE parallel_probe (
	cohort STRING NOT NULL,
	d8 FLOAT64 NOT NULL,
	d1 FLOAT64 NOT NULL,
	d6 FLOAT64 NOT NULL,
	d3 FLOAT64 NOT NULL,
	d7 FLOAT64 NOT NULL,
	d2 FLOAT64 NOT NULL,
	d5 FLOAT64 NOT NULL,
	d4 FLOAT64 NOT NULL
);

INSERT INTO parallel_probe VALUES
('West',  82.0, 14.0, 66.0, 31.0, 75.0, 22.0, 57.0, 43.0),
('East',  61.0, 27.0, 78.0, 45.0, 69.0, 35.0, 52.0, 88.0),
('West',  73.0, 19.0, 59.0, 38.0, 84.0, 29.0, 63.0, 51.0),
('East',  90.0, 33.0, 71.0, 56.0, 62.0, 41.0, 48.0, 77.0);
