-- PRODUCT-0103 pie surface exhibit data.
-- Idempotent: rerunning against the local Toron endpoint replaces the table.
-- Duplicate categories are intentional: the authored Rune relation aggregates
-- them, while the raw companion proves Polariton itself still refuses them.
DROP TABLE IF EXISTS pie_probe;

CREATE TABLE pie_probe (
	category STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO pie_probe VALUES
('Hardware', 35.0),
('Services', 30.0),
('Hardware', 15.0),
('Support', 10.0),
('Services', 10.0),
('Support', 5.0);
