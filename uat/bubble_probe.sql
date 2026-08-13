-- Purpose-built observations for the PRODUCT-0095 bubble exhibit.
-- Idempotent: rerunning against the local Toron endpoint replaces the table.
--
-- The colour domain is signed, the size domain has visibly different endpoints,
-- and the middle observation has a null label so the labelled figure exercises
-- PRODUCT-0090's faithful absent-label hover path.
DROP TABLE IF EXISTS bubble_probe;

CREATE TABLE bubble_probe (
	x FLOAT64 NOT NULL,
	y FLOAT64 NOT NULL,
	size FLOAT64 NOT NULL,
	color FLOAT64 NOT NULL,
	label STRING
);

INSERT INTO bubble_probe VALUES
(1.0, 2.0,      5.0, -30.0, 'Alpha'),
(2.0, 5.0,     50.0, -12.0, 'Beta'),
(3.0, 3.0,  50000.0,   0.0, NULL),
(4.0, 7.0,   5000.0,  15.0, 'Delta'),
(5.0, 4.0,    500.0,  28.0, 'Epsilon'),
(6.0, 8.0, 123456.0,  40.0, 'Zeta');
