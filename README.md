# ti1s3

Simple ET XML snapshot poller.

- Fetches Entur ET data every 20 seconds
- Keeps one constant `requestorId` *(for runtime)*
- Uploads raw XML to S3 as `YYYYMMDDHHmmss-et.xml`.

## Config

Use environment variables (same names for local `.env` and Docker Compose):

- `POLL_INTERVAL_SECONDS` (default: `20`)
- `RETENTION_HOURS` (default: `168` / 7 days)
- `USED_RETENTION_HOURS` (optional; if empty uses `RETENTION_HOURS`)
- `USED_FILES_CACHE_SECONDS` (default: `300`)
- `ENTUR_REQUESTOR_ID` (optional; if empty: `ti1s3-<startup timestamp>`)
- `ENTUR_BASE_URL` (default: `https://api.entur.io/realtime/v1/rest/et`)
- `API_KEYS` (optional; comma separated API keys for protected endpoints)
- `S3_ENDPOINT` (required)
- `S3_REGION` (default: `ume1`)
- `S3_BUCKET` (required)
- `S3_ACCESS_KEY` (required)
- `S3_SECRET_KEY` (required)
- `S3_PATH_STYLE` (default: `true`)
- `HEALTH_ADDR` (default: `:8080`)
- `HEALTH_PORT` (default: `8080`, Docker host port mapping)

## Health endpoints

- `GET /healthz` -> JSON details, returns `200` when healthy, `404` when not healthy.
- `GET /health-status` -> plain text only (`ok` or `not ok`), returns `200` or `404`.
- `GET /used-files` -> JSON list of files already marked as used.
- `POST /used-files/mark` -> mark one file as used for shorter retention.

If `API_KEYS` is set, send one key in either `X-API-Key` or `Authorization: Bearer <key>` for `/used-files` endpoints.

Example mark request:

```bash
curl -X POST http://localhost:8080/used-files/mark \
	-H "Content-Type: application/json" \
	-H "X-API-Key: change-me-key-1" \
	-d '{"key":"20260222121000-et.xml"}'
```

## Local development

1. Create env file:

2. Fill in S3 values in `.env`.

3. Run:

```bash
go run .
```

## Docker Compose deployment

it use the env

```bash
docker compose up -d --build
```

## Docker image publish (GitHub)

GitHub Actions builds and publishes the image to GitHub Container Registry (GHCR):

- Image: `ghcr.io/pigwin-3/ti1s3`
- On push to `main`: publishes `latest` and `sha-<commit>`

