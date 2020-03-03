BEGIN;

DROP INDEX track_mbid_idx_uniq;
DROP INDEX track_puid_idx_uniq;
DROP INDEX track_meta_idx_uniq;
DROP INDEX track_foreignid_idx_uniq;

COMMIT;
