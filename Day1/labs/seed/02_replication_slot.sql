-- Lab 1.1 step 5 measures WAL retained by a replication slot while the standby is
-- paused. pg_basebackup -R alone does NOT create a slot, so without this the
-- standby streams slotless, pg_replication_slots is empty, and the step has
-- nothing to show. pg_replica's entrypoint passes -S replica1 to adopt this slot.
SELECT pg_create_physical_replication_slot('replica1');
