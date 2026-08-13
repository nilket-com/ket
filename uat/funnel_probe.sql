-- Purpose-built stages for the PRODUCT-0098 funnel exhibit.
-- Idempotent: rerunning against the local Toron endpoint replaces both tables.
--
-- Paid and Renewed form an equal-valued plateau. Reversing only those rows
-- remains valid under PRODUCT-0096 while exercising label-bound colour identity.
-- The companion relation rises at its third stage and must refuse.
DROP TABLE IF EXISTS funnel_probe;
DROP TABLE IF EXISTS funnel_probe_rising;

CREATE TABLE funnel_probe (
	stage STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO funnel_probe VALUES
('Visited', 120.0),
('Trial',    80.0),
('Paid',     40.0),
('Renewed',  40.0);

CREATE TABLE funnel_probe_rising (
	stage STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO funnel_probe_rising VALUES
('Visited', 120.0),
('Trial',    60.0),
('Paid',     75.0);
