-- PRODUCT-0146's authored temporal fill-between exhibit.
-- Idempotent: rerunning replaces the healthy and crossed-band relations.
DROP TABLE IF EXISTS fill_between_probe;
DROP TABLE IF EXISTS fill_between_probe_crossed;

CREATE TABLE fill_between_probe (
	stamp TIMESTAMPTZ NOT NULL,
	low FLOAT64 NOT NULL,
	high FLOAT64 NOT NULL,
	expected FLOAT64 NOT NULL
);

INSERT INTO fill_between_probe VALUES
	(TIMESTAMPTZ '2026-09-01T00:00:00Z', 18.0, 26.0, 22.0),
	(TIMESTAMPTZ '2026-09-02T00:00:00Z', 19.0, 28.0, 23.5),
	(TIMESTAMPTZ '2026-09-03T00:00:00Z', 21.0, 30.0, 25.0),
	(TIMESTAMPTZ '2026-09-04T00:00:00Z', 20.0, 29.0, 24.0),
	(TIMESTAMPTZ '2026-09-05T00:00:00Z', 22.0, 32.0, 27.0);

CREATE TABLE fill_between_probe_crossed (
	x FLOAT64 NOT NULL,
	low FLOAT64 NOT NULL,
	high FLOAT64 NOT NULL
);

INSERT INTO fill_between_probe_crossed VALUES
	(1.0, 10.0, 14.0),
	(2.0, 16.0, 15.0),
	(3.0, 12.0, 17.0);
