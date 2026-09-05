-- Fixture data for the PRODUCT-0157 gate-10 checks the semantic showcases do
-- not cover: a percent-normalized stacked area, a hostile series label, and a
-- scatter dense enough to cross the automatic raster gate.
--
-- CTAS derives columns from the named select expressions. Build the raster
-- fixture in two stages, explicitly broadcasting the 105-row seed in each:
-- automatic broadcast admission does not accept the unhinted cross join on
-- the local deployment. No planner limits need changing.
DROP TABLE IF EXISTS percent_area_fixture;
DROP TABLE IF EXISTS hostile_label_fixture;
DROP TABLE IF EXISTS raster_scatter_fixture;
DROP TABLE IF EXISTS raster_seed_pairs;
DROP TABLE IF EXISTS raster_seed;

-- Three bands over eight periods. The lower band (Base) is the one to hide:
-- the gap it leaves must not move Middle or Top, and the percent totals must
-- keep their original normalization.
CREATE TABLE percent_area_fixture (
	period BIGINT NOT NULL,
	series STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO percent_area_fixture VALUES
	(1, 'Base', 40.0), (2, 'Base', 42.0), (3, 'Base', 45.0), (4, 'Base', 41.0),
	(5, 'Base', 38.0), (6, 'Base', 36.0), (7, 'Base', 39.0), (8, 'Base', 44.0),
	(1, 'Middle', 25.0), (2, 'Middle', 28.0), (3, 'Middle', 24.0), (4, 'Middle', 30.0),
	(5, 'Middle', 33.0), (6, 'Middle', 31.0), (7, 'Middle', 29.0), (8, 'Middle', 27.0),
	(1, 'Top', 10.0), (2, 'Top', 14.0), (3, 'Top', 18.0), (4, 'Top', 15.0),
	(5, 'Top', 21.0), (6, 'Top', 26.0), (7, 'Top', 24.0), (8, 'Top', 30.0);

-- One series label is the literal PRODUCT-0155 gate-6 hostile string. It must
-- reach the shell as text, keep one executable script element, and still
-- toggle from the legend.
CREATE TABLE hostile_label_fixture (
	period BIGINT NOT NULL,
	series STRING NOT NULL,
	value FLOAT64 NOT NULL
);

INSERT INTO hostile_label_fixture VALUES
	(1, 'Plain', 10.0), (2, 'Plain', 14.0), (3, 'Plain', 12.0), (4, 'Plain', 18.0),
	(1, '</script><script>alert(1)</script> & <', 6.0),
	(2, '</script><script>alert(1)</script> & <', 9.0),
	(3, '</script><script>alert(1)</script> & <', 15.0),
	(4, '</script><script>alert(1)</script> & <', 11.0);

-- 105 seed ordinals, cross-joined three ways: 1,157,625 points, above the
-- 1,000,000 raster threshold. In a pane under about 380 x 380 pixels that is
-- more than eight points per pixel, so the automatic gate rasterizes. Every
-- other pane size draws a downsample instead, and the figure is then covered.
CREATE TABLE raster_seed (
	i BIGINT NOT NULL
);

INSERT INTO raster_seed VALUES
	(0),
	(1),
	(2),
	(3),
	(4),
	(5),
	(6),
	(7),
	(8),
	(9),
	(10),
	(11),
	(12),
	(13),
	(14),
	(15),
	(16),
	(17),
	(18),
	(19),
	(20),
	(21),
	(22),
	(23),
	(24),
	(25),
	(26),
	(27),
	(28),
	(29),
	(30),
	(31),
	(32),
	(33),
	(34),
	(35),
	(36),
	(37),
	(38),
	(39),
	(40),
	(41),
	(42),
	(43),
	(44),
	(45),
	(46),
	(47),
	(48),
	(49),
	(50),
	(51),
	(52),
	(53),
	(54),
	(55),
	(56),
	(57),
	(58),
	(59),
	(60),
	(61),
	(62),
	(63),
	(64),
	(65),
	(66),
	(67),
	(68),
	(69),
	(70),
	(71),
	(72),
	(73),
	(74),
	(75),
	(76),
	(77),
	(78),
	(79),
	(80),
	(81),
	(82),
	(83),
	(84),
	(85),
	(86),
	(87),
	(88),
	(89),
	(90),
	(91),
	(92),
	(93),
	(94),
	(95),
	(96),
	(97),
	(98),
	(99),
	(100),
	(101),
	(102),
	(103),
	(104);

CREATE TABLE raster_seed_pairs AS SELECT /*+ BROADCAST(b) */
	a.i * 105 + b.i AS i
FROM raster_seed a CROSS JOIN raster_seed b;

CREATE TABLE raster_scatter_fixture AS SELECT /*+ BROADCAST(c) */
	p.i * 105 + c.i AS row_id,
	SEEDED_RANDOM(1, p.i * 105 + c.i) AS x,
	SEEDED_RANDOM(2, p.i * 105 + c.i) AS y,
	CASE MOD(p.i * 105 + c.i, CAST(3 AS BIGINT))
		WHEN 0 THEN 'Atlas'
		WHEN 1 THEN 'Beacon'
		ELSE 'Cinder'
	END AS series
FROM raster_seed_pairs p CROSS JOIN raster_seed c;
