-- PRODUCT-0114 Stage 0 long-axis-label probe.
--
-- The two facets deliberately carry unequal categorical labels. A future
-- grid-wide band must reserve once for the longest producer requirement, not
-- give each cell a private left or bottom inset.
DROP TABLE IF EXISTS axis_labels_probe;

CREATE TABLE axis_labels_probe (
	facet STRING NOT NULL,
	label STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO axis_labels_probe VALUES
	('Compact', 'Plan', 62.0),
	('Compact', 'Build', 74.0),
	('Compact', 'Ship', 58.0),
	('Compact', 'Learn', 81.0),
	('Verbose', 'North American enterprise renewal commitments', 68.0),
	('Verbose', 'International partner enablement and certification', 77.0),
	('Verbose', 'Self-service adoption among distributed teams', 54.0),
	('Verbose', 'Regulated migration and governance readiness', 72.0);
