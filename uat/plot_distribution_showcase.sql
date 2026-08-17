-- Presentation data for the Distribution & variation plot-type showcase.
--
-- The workbook derives every plotted relation from these raw observations:
-- grouped averages, fixed numeric bins, Tukey summaries, and kernel density.
DROP TABLE IF EXISTS distribution_observations;

CREATE TABLE distribution_observations (
	cohort STRING NOT NULL,
	score FLOAT64 NOT NULL
);

INSERT INTO distribution_observations VALUES
	('Atlas', 46.0), ('Atlas', 48.0), ('Atlas', 49.0), ('Atlas', 50.0),
	('Atlas', 51.0), ('Atlas', 52.0), ('Atlas', 53.0), ('Atlas', 54.0),
	('Atlas', 55.0), ('Atlas', 56.0), ('Atlas', 57.0), ('Atlas', 58.0),
	('Atlas', 59.0), ('Atlas', 60.0), ('Atlas', 61.0), ('Atlas', 75.0),
	('Beacon', 31.0), ('Beacon', 35.0), ('Beacon', 37.0), ('Beacon', 39.0),
	('Beacon', 40.0), ('Beacon', 41.0), ('Beacon', 42.0), ('Beacon', 43.0),
	('Beacon', 44.0), ('Beacon', 45.0), ('Beacon', 46.0), ('Beacon', 47.0),
	('Beacon', 48.0), ('Beacon', 49.0), ('Beacon', 50.0), ('Beacon', 52.0),
	('Cinder', 61.0), ('Cinder', 63.0), ('Cinder', 65.0), ('Cinder', 66.0),
	('Cinder', 67.0), ('Cinder', 68.0), ('Cinder', 69.0), ('Cinder', 70.0),
	('Cinder', 71.0), ('Cinder', 72.0), ('Cinder', 73.0), ('Cinder', 74.0),
	('Cinder', 75.0), ('Cinder', 76.0), ('Cinder', 78.0), ('Cinder', 90.0),
	('Delta', 39.0), ('Delta', 41.0), ('Delta', 43.0), ('Delta', 44.0),
	('Delta', 45.0), ('Delta', 46.0), ('Delta', 47.0), ('Delta', 48.0),
	('Delta', 49.0), ('Delta', 50.0), ('Delta', 51.0), ('Delta', 52.0),
	('Delta', 53.0), ('Delta', 54.0), ('Delta', 56.0), ('Delta', 68.0);
