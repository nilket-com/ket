-- Presentation data for the Multivariate & matrix plot-type showcase.
--
-- Heatmap rows are long-form cells; radar and parallel rows are deliberately
-- wide, one column per named operating dimension.
DROP TABLE IF EXISTS capability_matrix_showcase;
DROP TABLE IF EXISTS operating_profiles_showcase;

CREATE TABLE capability_matrix_showcase (
	journey STRING NOT NULL,
	capability STRING NOT NULL,
	delta FLOAT64 NOT NULL
);

INSERT INTO capability_matrix_showcase VALUES
	('Discover', 'Reliability',  18.0), ('Discover', 'Speed',  12.0),
	('Discover', 'Clarity',       6.0), ('Discover', 'Reach', -8.0),
	('Discover', 'Support',      -4.0),
	('Evaluate', 'Reliability',  11.0), ('Evaluate', 'Speed', -6.0),
	('Evaluate', 'Clarity',      15.0), ('Evaluate', 'Reach',  4.0),
	('Evaluate', 'Support',       9.0),
	('Adopt',    'Reliability',   4.0), ('Adopt',    'Speed', 18.0),
	('Adopt',    'Clarity',      -9.0), ('Adopt',    'Reach', 12.0),
	('Adopt',    'Support',      16.0),
	('Expand',   'Reliability',  14.0), ('Expand',   'Speed',  7.0),
	('Expand',   'Clarity',      10.0), ('Expand',   'Reach', 19.0),
	('Expand',   'Support',       5.0);

CREATE TABLE operating_profiles_showcase (
	team STRING NOT NULL,
	quality FLOAT64 NOT NULL,
	speed FLOAT64 NOT NULL,
	reliability FLOAT64 NOT NULL,
	reach FLOAT64 NOT NULL,
	support FLOAT64 NOT NULL
);

INSERT INTO operating_profiles_showcase VALUES
	('Atlas',   82.0, 74.0, 91.0, 68.0, 79.0),
	('Beacon', 68.0, 88.0, 72.0, 84.0, 63.0),
	('Cinder', 91.0, 61.0, 85.0, 57.0, 88.0),
	('Delta',  76.0, 81.0, 66.0, 92.0, 71.0);
