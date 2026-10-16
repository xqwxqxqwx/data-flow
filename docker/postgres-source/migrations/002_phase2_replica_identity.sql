-- Phase 2: full row images in WAL so CDC events carry `before` on UPDATE/DELETE.
-- Idempotent. Apply to an existing volume:
--   docker compose exec -T postgres-source psql -U source -d source -v ON_ERROR_STOP=1 \
--     < docker/postgres-source/migrations/002_phase2_replica_identity.sql

ALTER TABLE relic.customers      REPLICA IDENTITY FULL;
ALTER TABLE relic.items          REPLICA IDENTITY FULL;
ALTER TABLE relic.vans           REPLICA IDENTITY FULL;
ALTER TABLE relic.stop_locations REPLICA IDENTITY FULL;
ALTER TABLE relic.van_stops      REPLICA IDENTITY FULL;
ALTER TABLE relic.orders         REPLICA IDENTITY FULL;
ALTER TABLE relic.order_items    REPLICA IDENTITY FULL;
ALTER TABLE relic.deliveries     REPLICA IDENTITY FULL;
