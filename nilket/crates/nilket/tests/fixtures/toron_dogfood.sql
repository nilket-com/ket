-- seed before the restart boundary
CREATE TABLE dogfood (id BIGINT NOT NULL, label TEXT);
INSERT INTO dogfood (id, label) VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- prove durable rows and catalog provenance after restart
SELECT * FROM dogfood;
SELECT * FROM information_schema.schemata;
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;

-- record supported execution or a typed full-text refusal for each operation
SELECT label, SUM(id) AS total FROM dogfood GROUP BY label;
UPDATE dogfood SET label = 'uno' WHERE id = 1;
DELETE FROM dogfood WHERE id = 2;
DROP TABLE dogfood;
