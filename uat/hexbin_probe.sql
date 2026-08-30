-- PRODUCT-0144's viewport-prepared hexbin exhibit.
-- Idempotent: rerunning replaces the healthy and temporal-refusal relations.
DROP TABLE IF EXISTS hexbin_probe;
DROP TABLE IF EXISTS hexbin_probe_temporal;

CREATE TABLE hexbin_probe (
	x FLOAT64 NOT NULL,
	y FLOAT64 NOT NULL
);

-- Several tight clusters make repeated occupancy visible, while the separated
-- observations ensure no viewport lattice can collapse all N rows into one bin.
INSERT INTO hexbin_probe VALUES
	(0.00, 0.00), (0.01, 0.01), (0.02, 0.01), (0.01, 0.02),
	(0.42, 0.38), (0.43, 0.39), (0.44, 0.38), (0.43, 0.40),
	(0.78, 0.82), (0.79, 0.81), (0.80, 0.83),
	(1.00, 0.10), (0.10, 1.00), (1.00, 1.00);

CREATE TABLE hexbin_probe_temporal (
	stamp TIMESTAMPTZ NOT NULL,
	y FLOAT64 NOT NULL
);

INSERT INTO hexbin_probe_temporal VALUES
	(TIMESTAMPTZ '2026-08-30T12:00:00Z', 0.25),
	(TIMESTAMPTZ '2026-08-30T13:00:00Z', 0.75);
