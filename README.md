# Leads Sync Machine (Supabase + Fastify)

Starter Node 20+ TypeScript server that upserts normalized data and ingests Apify job payloads into Supabase Postgres with idempotent natural keys.

## Setup

1. Install deps (from repo root):

```
npm install
```

2. Copy env template and fill Supabase project URL + service role key:

```
cp env.example .env
```

3. Apply migration `migrations/001_init.sql` in Supabase SQL editor or via `psql`.
4. Run dev server:

```
npm run dev
```

Or build and run:

```
npm run build && npm start
```

Server listens on `PORT` (default 3000). Health check: `GET /health`.

## Endpoints

### POST /import/normalized

Upserts normalized companies, people, and company_people links. Idempotent on natural keys.

Example:

```
curl -X POST http://localhost:3000/import/normalized \
  -H "Content-Type: application/json" \
  -d '{
    "companies": [
      {"company_key":"domain:finnas-kraftlag.no","name":"Finnås Kraftlag","domain_host":"finnas-kraftlag.no","orgnr":"912345678","industry":"Energy"}
    ],
    "people": [
      {"person_key":"em:ola@finnas-kraftlag.no","full_name":"Ola Nordmann","title":"CTO","email":"ola@finnas-kraftlag.no","phone":"+47 12345678"}
    ],
    "links": [
      {"company_key":"domain:finnas-kraftlag.no","person_key":"em:ola@finnas-kraftlag.no","role":"decision_maker"}
    ]
  }'
```

Response contains inserted/updated counts for each table.

### POST /ingest/apify-job

Normalizes a single Apify job payload, derives keys, upserts company/job/people and links relations.

Example:

```
curl -X POST http://localhost:3000/ingest/apify-job \
  -H "Content-Type: application/json" \
  -d '{
    "url":"https://www.finn.no/job/ad/445216243",
    "title":"Senior Engineer",
    "description":"Build cool things",
    "company":"ACME Energy",
    "domain":"https://acme-energy.no",
    "contactPersons":[{"name":"Kari Nordmann","phoneNumber":"+47 99988877"}],
    "applicationUrl":"https://acme-energy.no/jobs/445216243",
    "location":"Oslo",
    "employmentType":"Full-time",
    "publicationDate":"2024-05-01T12:00:00Z"
  }'
```

Response returns upsert counts for companies, job_posts, people, job_post_people, and company_people.

## Notes

- Uses Fastify + pino logging, rate limiting (120 req/min), 2MB body limit.
- Natural keys only: company_key (orgnr|domain|name slug), finn_id from Finn URL, person_key (linkedin|email|phone|nm fallback).
- All writes are idempotent; repeating the same payload is safe.
