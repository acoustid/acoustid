package export

type QueryContext struct {
	StartTime string
	EndTime   string
}

const ExportFingerprintDeltaQuery = `
SELECT id, fingerprint, length, created
FROM fingerprint
WHERE created >= '{{.StartTime}}' AND created < '{{.EndTime}}'
`

const ExportMetaDeltaQuery = `
SELECT id, track, artist, album, album_artist, track_no, disc_no, year, created
FROM meta
WHERE created >= '{{.StartTime}}' AND created < '{{.EndTime}}'
`
