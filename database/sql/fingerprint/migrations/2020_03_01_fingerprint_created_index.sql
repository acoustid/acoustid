BEGIN;

CREATE INDEX fingerprint_idx_created ON fingerprint (created);

COMMIT;
