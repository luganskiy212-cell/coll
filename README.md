# KINGMAKER Collector

Small 24/7 worker for KINGMAKER.

It connects to the PumpPortal WebSocket using **one connection** and subscribes only to the free streams:

- `subscribeNewToken`
- `subscribeMigration`

New Pump.fun launches are written into the Supabase `tokens` table. Migrations mark the token as `graduated=true`.

## Required environment variables

Set these in your hosting provider (Railway), never in GitHub:

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY` (recommended, starts with `sb_secret_...`)
  - or legacy `SUPABASE_SERVICE_ROLE_KEY`
- `PUMPPORTAL_API_KEY`

Optional:

- `PORT` (Railway normally supplies this automatically)

## Run

```bash
npm install
npm start
```

## Health check

Open `/health`. You should see counters such as `createsSaved`, `migrationsSaved`, and `lastCreateAt`.

## Supabase check

Run this in Supabase SQL Editor:

```sql
select mint, creator, name, symbol, graduated, created_at
from tokens
order by created_at desc
limit 20;
```

If the collector is healthy, new Pump.fun launches should begin appearing.
