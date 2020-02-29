package export

const ExportFingerprintDeltaQuery = `
SELECT id, fingerprint, length, created
FROM fingerprint
WHERE created >= '{{startTime}}' AND created < '{{endTime}}'
`
