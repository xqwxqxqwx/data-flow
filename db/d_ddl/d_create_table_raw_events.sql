--cat C:\Users\PC\Documents\PY_proj\'git test'\data-flow\db\d_ddl\d_create_table_raw_events.sql | docker exec -i !cont_name! psql -U source -d source
-- -i = cont name
-- -U = user name
-- -d = db   name

-- d_create_table_raw_events.sql
-- Назначение: Создание таблицы для приёма сырых событий
-- Автор: Krio
-- Дата: 2026-05-05

DROP TABLE IF EXISTS raw.raw_events CASCADE;

-- Идемпотентность: безопасно запускать повторно
CREATE TABLE IF NOT EXISTS d_raw.raw_events (
    event_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    event_type VARCHAR(50),
    created_at TIMESTAMP
);

-- Индексы под типичные аналитические запросы
--CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON raw_events(user_id);
--CREATE INDEX IF NOT EXISTS idx_raw_events_created_at ON raw_events(created_at);

COMMENT ON TABLE d_raw.raw_events IS 'Сырые события активности пользователей';