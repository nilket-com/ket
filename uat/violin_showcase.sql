-- Presentation-focused violin exhibit data.
--
-- Unlike violin_probe, this is not a degenerate-policy stress fixture. Seven
-- cohorts have enough observations to make their distinct density shapes easy
-- to read on one shared evaluation and width scale:
--
--   Aurora   balanced, compact bell
--   Boreal   two separated modes
--   Cascade  concentrated body with a high-value tail
--   Drift    concentrated body with a low-value tail
--   Ember    broad, nearly even plateau
--   Fjord    very compact central cohort
--   Grove    central body with a gentle upper shoulder
--
-- The table is idempotent so the showcase can be reseeded independently of the
-- PRODUCT-0107 probe.
DROP TABLE IF EXISTS violin_showcase;

CREATE TABLE violin_showcase (
	cohort STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO violin_showcase VALUES
	('Aurora', 38.0),
	('Aurora', 43.0),
	('Aurora', 47.0),
	('Aurora', 50.0),
	('Aurora', 52.0),
	('Aurora', 53.0),
	('Aurora', 54.0),
	('Aurora', 55.0),
	('Aurora', 56.0),
	('Aurora', 58.0),
	('Aurora', 61.0),
	('Aurora', 65.0),
	('Boreal', 26.0),
	('Boreal', 28.0),
	('Boreal', 30.0),
	('Boreal', 31.0),
	('Boreal', 33.0),
	('Boreal', 35.0),
	('Boreal', 61.0),
	('Boreal', 63.0),
	('Boreal', 65.0),
	('Boreal', 66.0),
	('Boreal', 68.0),
	('Boreal', 70.0),
	('Cascade', 34.0),
	('Cascade', 36.0),
	('Cascade', 38.0),
	('Cascade', 39.0),
	('Cascade', 40.0),
	('Cascade', 41.0),
	('Cascade', 42.0),
	('Cascade', 43.0),
	('Cascade', 45.0),
	('Cascade', 49.0),
	('Cascade', 58.0),
	('Cascade', 76.0),
	('Drift', 24.0),
	('Drift', 40.0),
	('Drift', 51.0),
	('Drift', 55.0),
	('Drift', 57.0),
	('Drift', 58.0),
	('Drift', 59.0),
	('Drift', 60.0),
	('Drift', 61.0),
	('Drift', 62.0),
	('Drift', 64.0),
	('Drift', 67.0),
	('Ember', 33.0),
	('Ember', 36.0),
	('Ember', 39.0),
	('Ember', 42.0),
	('Ember', 45.0),
	('Ember', 48.0),
	('Ember', 51.0),
	('Ember', 54.0),
	('Ember', 57.0),
	('Ember', 60.0),
	('Ember', 63.0),
	('Ember', 66.0),
	('Fjord', 47.0),
	('Fjord', 48.0),
	('Fjord', 49.0),
	('Fjord', 49.5),
	('Fjord', 50.0),
	('Fjord', 50.5),
	('Fjord', 51.0),
	('Fjord', 51.5),
	('Fjord', 52.0),
	('Fjord', 53.0),
	('Grove', 32.0),
	('Grove', 37.0),
	('Grove', 42.0),
	('Grove', 46.0),
	('Grove', 48.0),
	('Grove', 49.0),
	('Grove', 50.0),
	('Grove', 51.0),
	('Grove', 53.0),
	('Grove', 57.0),
	('Grove', 63.0),
	('Grove', 70.0);
