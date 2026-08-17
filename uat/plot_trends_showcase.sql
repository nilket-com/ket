-- Presentation data for the Trends & relationships plot-type showcase.
--
-- One relation serves every mark in the workbook: period orders the trend,
-- series distinguishes the three product lines, value supplies the vertical
-- measure, and magnitude gives bubbles a second quantitative channel.
DROP TABLE IF EXISTS trend_showcase;

CREATE TABLE trend_showcase (
	period BIGINT NOT NULL,
	series STRING NOT NULL,
	value FLOAT64 NOT NULL,
	magnitude FLOAT64 NOT NULL
);

INSERT INTO trend_showcase VALUES
	(1, 'Northstar', 32.0, 18.0),
	(2, 'Northstar', 38.0, 22.0),
	(3, 'Northstar', 44.0, 29.0),
	(4, 'Northstar', 52.0, 36.0),
	(5, 'Northstar', 60.0, 43.0),
	(6, 'Northstar', 66.0, 51.0),
	(7, 'Northstar', 73.0, 62.0),
	(8, 'Northstar', 81.0, 76.0),
	(1, 'Horizon', 61.0, 74.0),
	(2, 'Horizon', 58.0, 68.0),
	(3, 'Horizon', 55.0, 61.0),
	(4, 'Horizon', 53.0, 57.0),
	(5, 'Horizon', 49.0, 50.0),
	(6, 'Horizon', 46.0, 43.0),
	(7, 'Horizon', 44.0, 37.0),
	(8, 'Horizon', 41.0, 32.0),
	(1, 'Pulse', 20.0, 12.0),
	(2, 'Pulse', 25.0, 17.0),
	(3, 'Pulse', 22.0, 15.0),
	(4, 'Pulse', 31.0, 24.0),
	(5, 'Pulse', 39.0, 35.0),
	(6, 'Pulse', 35.0, 30.0),
	(7, 'Pulse', 49.0, 47.0),
	(8, 'Pulse', 57.0, 63.0);
