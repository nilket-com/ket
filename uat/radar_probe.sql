-- PRODUCT-0105 radar surface exhibit data.
-- Interleaved category rows make global relation order distinct from grouping,
-- while category presentation remains lexical.
DROP TABLE IF EXISTS radar_probe;

CREATE TABLE radar_probe (
	team STRING NOT NULL,
	quality FLOAT64 NOT NULL,
	speed FLOAT64 NOT NULL,
	reliability FLOAT64 NOT NULL,
	reach FLOAT64 NOT NULL
);

INSERT INTO radar_probe VALUES
('Zulu',  72.0, 88.0, 64.0, 79.0),
('Alpha', 91.0, 63.0, 86.0, 70.0),
('Zulu',  81.0, 76.0, 73.0, 84.0),
('Alpha', 77.0, 82.0, 69.0, 93.0);
