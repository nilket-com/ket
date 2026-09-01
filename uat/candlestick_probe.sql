-- PRODUCT-0148's authored temporal candlestick exhibit.
-- Idempotent: rerunning replaces the healthy and torn-containment relations.
DROP TABLE IF EXISTS candlestick_probe;
DROP TABLE IF EXISTS candlestick_probe_torn;

CREATE TABLE candlestick_probe (
	stamp TIMESTAMPTZ NOT NULL,
	open FLOAT64 NOT NULL,
	high FLOAT64 NOT NULL,
	low FLOAT64 NOT NULL,
	close FLOAT64 NOT NULL
);

INSERT INTO candlestick_probe VALUES
	(TIMESTAMPTZ '2026-09-03T14:30:00Z', 100.0, 106.0, 98.0, 104.0),
	(TIMESTAMPTZ '2026-09-04T14:30:00Z', 104.0, 105.0, 96.0, 99.0),
	(TIMESTAMPTZ '2026-09-07T14:30:00Z', 101.0, 105.0, 97.0, 101.0),
	(TIMESTAMPTZ '2026-09-08T14:30:00Z', 101.0, 109.0, 100.0, 107.0);

CREATE TABLE candlestick_probe_torn (
	stamp TIMESTAMPTZ NOT NULL,
	open FLOAT64 NOT NULL,
	high FLOAT64 NOT NULL,
	low FLOAT64 NOT NULL,
	close FLOAT64 NOT NULL
);

INSERT INTO candlestick_probe_torn VALUES
	(TIMESTAMPTZ '2026-09-03T14:30:00Z', 100.0, 106.0, 98.0, 104.0),
	(TIMESTAMPTZ '2026-09-04T14:30:00Z', 104.0, 101.0, 96.0, 103.0),
	(TIMESTAMPTZ '2026-09-07T14:30:00Z', 101.0, 105.0, 97.0, 101.0);
