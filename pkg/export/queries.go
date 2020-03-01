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
