# Pinned CA certificates

Public CA certificates the services trust for specific endpoints. Not secrets.

| File | Trusts | SHA-256 fingerprint | Expires |
| --- | --- | --- | --- |
| `supabase-prod-ca-2021.crt` | Supabase Root 2021 CA: Supabase Postgres and the Supavisor pooler (`*.pooler.supabase.com`) | `80:70:25:AD:50:D4:ED:21:9D:2C:9C:7D:29:9C:00:4F:82:4E:B0:0C:F7:F6:5A:FE:F6:07:D0:7B:72:E6:CA:FA` | 2031-04-26 |

Source: `https://supabase-downloads.s3-ap-southeast-1.amazonaws.com/prod/ssl/prod-ca-2021.crt`,
checked on 2026-10-29 to be byte-identical to the root the `aws-1-us-west-2` pooler presents
(chain: `*.pooler.supabase.com` -> Supabase Intermediate 2021 CA -> this root).

The services image ships it at `/app/certs/`. HoopDb URLs for Supabase use
`sslmode=verify-full&sslrootcert=/app/certs/supabase-prod-ca-2021.crt`.
