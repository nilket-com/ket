-- PRODUCT-0107 violin substrate exhibit data.
-- Idempotent: rerunning against the local Toron endpoint replaces the table.
--
-- Four groups chosen to exercise Toron ADR 552's delivered policy rather than a
-- single happy path:
--
--   tight     small spread, so its bandwidth is narrow and its peak is tall
--   diffuse   wide spread over the same count, so its curve is low and broad
--   plateau   more than half its values identical with genuine tail spread,
--             which drives 552's zero-IQR fallback to (p90 - p10) / 2.563
--   lonely    one observation, which 552 emits no rows for at all
--
-- `lonely` is the point of the fixture, not an oversight: PRODUCT-0107 §4 says
-- an absent group stays absent, so the prepared relation must simply not carry
-- it, and nothing downstream may invent a flat violin in its place.
DROP TABLE IF EXISTS violin_probe;

CREATE TABLE violin_probe (
	region STRING NOT NULL,
	measure FLOAT64 NOT NULL
);

INSERT INTO violin_probe VALUES
('tight', 49.0),
('tight', 50.0),
('tight', 50.0),
('tight', 51.0),
('tight', 51.0),
('tight', 52.0),
('diffuse', 10.0),
('diffuse', 28.0),
('diffuse', 45.0),
('diffuse', 55.0),
('diffuse', 72.0),
('diffuse', 90.0),
('plateau', 30.0),
('plateau', 30.0),
('plateau', 30.0),
('plateau', 30.0),
('plateau', 12.0),
('plateau', 48.0),
('lonely', 65.0);
