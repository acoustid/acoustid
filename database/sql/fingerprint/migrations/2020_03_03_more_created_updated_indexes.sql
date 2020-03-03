CREATE INDEX CONCURRENTLY fingerprint_idx_updated ON fingerprint (updated) WHERE updated IS NOT NULL;

CREATE INDEX CONCURRENTLY track_idx_created ON track (created);
CREATE INDEX CONCURRENTLY track_idx_updated ON track (updated) WHERE updated IS NOT NULL;

CREATE INDEX CONCURRENTLY track_puid_idx_created ON track_puid (created);
CREATE INDEX CONCURRENTLY track_puid_idx_updated ON track_puid (updated) WHERE updated IS NOT NULL;

CREATE INDEX CONCURRENTLY track_mbid_idx_created ON track_mbid (created);
CREATE INDEX CONCURRENTLY track_mbid_idx_updated ON track_mbid (updated) WHERE updated IS NOT NULL;
