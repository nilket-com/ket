-- Operator dogfood only. Observed runs are not the total population.
-- Refresh these tables before publishing journey_business_view.rn.
DROP TABLE IF EXISTS journey_runs;
CREATE TABLE journey_runs AS
SELECT install_id, run_id, platform,
 MAX(CASE WHEN step = 'run.opened' THEN 1 ELSE 0 END) AS ran,
 MAX(CASE WHEN step = 'own_file.opened' AND outcome = 'ok' THEN 1 ELSE 0 END) AS opened,
 MAX(CASE WHEN step = 'first_analysis.kept' THEN 1 ELSE 0 END) AS kept,
 MIN(CASE WHEN step = 'run.opened' THEN at_ms ELSE NULL END) AS started,
 MIN(CASE WHEN step = 'first_analysis.kept' THEN at_ms ELSE NULL END) AS kept_at,
 MAX(sequence) AS last_sequence
FROM journey_reports GROUP BY install_id, run_id, platform;
DROP TABLE IF EXISTS journey_states;
CREATE TABLE journey_states AS
SELECT 'Run' AS step, CASE WHEN ran = 1 THEN 'observed' WHEN opened = 1 OR kept = 1 THEN 'absent' ELSE 'unknown' END AS state FROM journey_runs;
INSERT INTO journey_states
SELECT 'Own file', CASE WHEN opened = 1 THEN 'observed' WHEN kept = 1 THEN 'absent' ELSE 'unknown' END FROM journey_runs;
INSERT INTO journey_states
SELECT 'Kept analysis', CASE WHEN kept = 1 THEN 'observed' ELSE 'unknown' END FROM journey_runs;
DROP TABLE IF EXISTS journey_funnel;
CREATE TABLE journey_funnel AS
SELECT step, state, COUNT(*) AS runs FROM journey_states GROUP BY step, state;
DROP TABLE IF EXISTS journey_failures;
CREATE TABLE journey_failures (step VARCHAR, reason VARCHAR, platform VARCHAR, reports BIGINT);
INSERT INTO journey_failures
SELECT step, reason, platform, COUNT(*) AS reports
FROM journey_reports WHERE outcome = 'failed' GROUP BY step, reason, platform;
DROP TABLE IF EXISTS journey_reporting;
CREATE TABLE journey_reporting AS
SELECT platform, step, COUNT(*) AS runs FROM journey_reports
WHERE step = 'run.opened' OR step = 'reporting.disabled' GROUP BY platform, step;
DROP TABLE IF EXISTS journey_elapsed;
CREATE TABLE journey_elapsed (platform VARCHAR, started BIGINT, seconds DOUBLE);
INSERT INTO journey_elapsed
SELECT platform, started, CAST(kept_at - started AS DOUBLE) / CAST(1000 AS DOUBLE) AS seconds FROM journey_runs WHERE kept_at >= started;
DROP TABLE IF EXISTS journey_last_rows;
CREATE TABLE journey_last_rows AS
SELECT r.platform, r.step FROM journey_reports r JOIN journey_runs l
ON r.install_id = l.install_id AND r.run_id = l.run_id AND r.sequence = l.last_sequence;
DROP TABLE IF EXISTS journey_last_observed;
CREATE TABLE journey_last_observed AS
SELECT platform, step, COUNT(*) AS runs FROM journey_last_rows GROUP BY platform, step;
