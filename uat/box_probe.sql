-- Purpose-built observations for the PRODUCT-0081/0082/0083 box-plot exhibit.
-- Idempotent: rerunning against the local Toron endpoint replaces the table.
--
-- The rows deliberately cover five different Tukey shapes:
--   lowhigh  q1=31, median=33, q3=34; outliers -50 and 200
--   none     q1=21, median=23, q3=24; no outliers
--   twin     q1=12, median=14, q3=16; duplicate outliers 99 and 99
--   single   one observation, so every summary collapses to 2
--   flat     four identical observations, so every summary collapses to 5
--
-- Keep the two twin/99 rows distinct. Their multiplicity is the fixture for
-- deterministic band-axis offsets and must survive the complete Tukey query.
DROP TABLE IF EXISTS box_probe;

CREATE TABLE box_probe (
	grp STRING NOT NULL,
	v FLOAT64 NOT NULL
);

INSERT INTO box_probe VALUES
('lowhigh', -50.0),
('lowhigh', 30.0),
('lowhigh', 31.0),
('lowhigh', 32.0),
('lowhigh', 33.0),
('lowhigh', 34.0),
('lowhigh', 35.0),
('lowhigh', 200.0),

('none', 20.0),
('none', 21.0),
('none', 22.0),
('none', 23.0),
('none', 24.0),
('none', 25.0),

('twin', 10.0),
('twin', 11.0),
('twin', 12.0),
('twin', 13.0),
('twin', 14.0),
('twin', 15.0),
('twin', 16.0),
('twin', 99.0),
('twin', 99.0),

('single', 2.0),

('flat', 5.0),
('flat', 5.0),
('flat', 5.0),
('flat', 5.0);
