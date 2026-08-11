-- Demo relation for Polariton's faceting and AI-oriented query dogfood.
-- Idempotent: rerunning against the local Toron endpoint (127.0.0.1:7742)
-- replaces the table rather than appending, so rows cannot duplicate.
--
-- Four quarters per (region, product). Quarterly revenue and units sum
-- EXACTLY to the single-quarter fixture this replaced, so every view that
-- groups by region alone is unchanged. Each region carries a different
-- seasonal shape (North rising, South falling, East Q2-peaked, West
-- Q4-peaked) so a facet or quarter misassignment is visible rather than
-- plausible.
DROP TABLE IF EXISTS demo_sales;

CREATE TABLE demo_sales (
	region STRING,
	product STRING,
	quarter STRING,
	revenue FLOAT32,
	units BIGINT,
	margin FLOAT32
);

INSERT INTO demo_sales VALUES
('East', 'Atlas', 'Q1', 28.2, 9, 0.32),
('East', 'Atlas', 'Q2', 45.2, 15, 0.34),
('East', 'Atlas', 'Q3', 36.7, 12, 0.35),
('East', 'Atlas', 'Q4', 31.1, 10, 0.36),
('East', 'Beacon', 'Q1', 20.3, 7, 0.26),
('East', 'Beacon', 'Q2', 32.5, 12, 0.28),
('East', 'Beacon', 'Q3', 26.4, 10, 0.29),
('East', 'Beacon', 'Q4', 22.3, 8, 0.30),
('East', 'Cinder', 'Q1', 16.4, 6, 0.22),
('East', 'Cinder', 'Q2', 26.3, 10, 0.24),
('East', 'Cinder', 'Q3', 21.3, 8, 0.25),
('East', 'Cinder', 'Q4', 18.1, 6, 0.26),
('East', 'Delta', 'Q1', 11.7, 4, 0.17),
('East', 'Delta', 'Q2', 18.7, 7, 0.19),
('East', 'Delta', 'Q3', 15.2, 6, 0.20),
('East', 'Delta', 'Q4', 12.8, 5, 0.21),
('North', 'Atlas', 'Q1', 24.4, 8, 0.29),
('North', 'Atlas', 'Q2', 29.5, 10, 0.31),
('North', 'Atlas', 'Q3', 36.0, 12, 0.32),
('North', 'Atlas', 'Q4', 38.5, 12, 0.33),
('North', 'Beacon', 'Q1', 18.4, 6, 0.25),
('North', 'Beacon', 'Q2', 22.3, 7, 0.27),
('North', 'Beacon', 'Q3', 27.1, 9, 0.28),
('North', 'Beacon', 'Q4', 29.0, 9, 0.29),
('North', 'Cinder', 'Q1', 14.1, 5, 0.20),
('North', 'Cinder', 'Q2', 17.1, 6, 0.22),
('North', 'Cinder', 'Q3', 20.8, 8, 0.23),
('North', 'Cinder', 'Q4', 22.2, 9, 0.24),
('North', 'Delta', 'Q1', 9.8, 4, 0.16),
('North', 'Delta', 'Q2', 11.9, 4, 0.18),
('North', 'Delta', 'Q3', 14.4, 5, 0.19),
('North', 'Delta', 'Q4', 15.5, 6, 0.20),
('South', 'Atlas', 'Q1', 34.9, 12, 0.27),
('South', 'Atlas', 'Q2', 30.4, 11, 0.29),
('South', 'Atlas', 'Q3', 25.9, 9, 0.30),
('South', 'Atlas', 'Q4', 21.5, 7, 0.31),
('South', 'Beacon', 'Q1', 27.4, 11, 0.23),
('South', 'Beacon', 'Q2', 23.8, 9, 0.25),
('South', 'Beacon', 'Q3', 20.3, 8, 0.26),
('South', 'Beacon', 'Q4', 16.8, 6, 0.27),
('South', 'Cinder', 'Q1', 21.5, 8, 0.19),
('South', 'Cinder', 'Q2', 18.8, 7, 0.21),
('South', 'Cinder', 'Q3', 16.0, 6, 0.22),
('South', 'Cinder', 'Q4', 13.2, 5, 0.23),
('South', 'Delta', 'Q1', 14.8, 5, 0.14),
('South', 'Delta', 'Q2', 12.9, 5, 0.16),
('South', 'Delta', 'Q3', 11.0, 4, 0.17),
('South', 'Delta', 'Q4', 9.2, 3, 0.18),
('West', 'Atlas', 'Q1', 21.5, 7, 0.28),
('West', 'Atlas', 'Q2', 26.3, 9, 0.30),
('West', 'Atlas', 'Q3', 31.1, 11, 0.31),
('West', 'Atlas', 'Q4', 40.7, 14, 0.32),
('West', 'Beacon', 'Q1', 16.9, 6, 0.24),
('West', 'Beacon', 'Q2', 20.6, 8, 0.26),
('West', 'Beacon', 'Q3', 24.4, 9, 0.27),
('West', 'Beacon', 'Q4', 31.8, 12, 0.28),
('West', 'Cinder', 'Q1', 14.0, 5, 0.21),
('West', 'Cinder', 'Q2', 17.1, 6, 0.23),
('West', 'Cinder', 'Q3', 20.2, 8, 0.24),
('West', 'Cinder', 'Q4', 26.5, 10, 0.25),
('West', 'Delta', 'Q1', 9.8, 4, 0.15),
('West', 'Delta', 'Q2', 11.9, 4, 0.17),
('West', 'Delta', 'Q3', 14.1, 5, 0.18),
('West', 'Delta', 'Q4', 18.5, 7, 0.19);
