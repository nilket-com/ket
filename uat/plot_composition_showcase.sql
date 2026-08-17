-- Presentation data for the Composition & progress plot-type showcase.
--
-- The conversion relation is intentionally ordered and monotonically declining:
-- the same stage volumes are meaningful as a funnel and as parts of a whole.
DROP TABLE IF EXISTS conversion_showcase;
DROP TABLE IF EXISTS readiness_showcase;

CREATE TABLE conversion_showcase (
	stage_order BIGINT NOT NULL,
	stage STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO conversion_showcase VALUES
	(1, 'Awareness', 12000.0),
	(2, 'Qualified', 7200.0),
	(3, 'Evaluation', 4200.0),
	(4, 'Proposal', 2400.0),
	(5, 'Closed', 1200.0);

CREATE TABLE readiness_showcase (
	label STRING NOT NULL,
	value FLOAT64 NOT NULL,
	maximum FLOAT64 NOT NULL
);

INSERT INTO readiness_showcase VALUES
	('Reliability', 91.0, 100.0),
	('Documentation', 78.0, 100.0),
	('Training', 64.0, 100.0),
	('Rollout', 72.0, 100.0);
