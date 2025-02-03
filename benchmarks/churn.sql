\set id random(1, 10000000)
INSERT INTO test VALUES(:id, random(), random(), random(), random(), now() - random() * random() * 1800 * interval '1 second')
ON CONFLICT (id) DO UPDATE SET ts = now();