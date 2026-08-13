-- PRODUCT-0100 radial-dial surface exhibit data.
-- Idempotent: rerunning against the local Toron endpoint replaces both tables.
DROP TABLE IF EXISTS radial_dial_probe;
DROP TABLE IF EXISTS radial_dial_ungrouped;

CREATE TABLE radial_dial_probe (
	label STRING NOT NULL,
	value FLOAT64 NOT NULL,
	maximum FLOAT64 NOT NULL
);

-- Deliberately not lexical. PRODUCT-0099 sorts these presentation categories.
INSERT INTO radial_dial_probe VALUES
('West',  58.0, 100.0),
('East',  82.0, 100.0),
('South', 36.0, 100.0),
('North', 71.0, 100.0);

CREATE TABLE radial_dial_ungrouped (
	value FLOAT64 NOT NULL,
	maximum FLOAT64 NOT NULL
);

INSERT INTO radial_dial_ungrouped VALUES (63.0, 100.0);
