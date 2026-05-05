--cat 'C:\Users\PC\Documents\PY_proj\git test\data-flow\db\d_dml\d_insert_data_raw_events.sql' | docker exec -i !cont_name! psql -U source -d source
-- -i = cont name
-- -U = user name
-- -d = db   name

-- d_insert_data_raw_events.sql
-- Назначение: Наполнение тестовыми данными
-- В проде обычно используется COPY из CSV или идемпотентный MERGE/UPSERT

INSERT INTO d_raw.raw_events
(event_id, user_id, event_type, created_at) VALUES
(1, 1, 'join', NOW()),
(2, 2, 'join', NOW()),
(3, 2, 'rename', NOW());