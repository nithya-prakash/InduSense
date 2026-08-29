# 11. Dashboard

[frontend/](../../frontend/) is a Next.js 16 + TypeScript app (App Router,
Turbopack, Tailwind CSS) covering every page the spec asks for: Overview,
Factory drill-down, Machine detail with live telemetry charts, Alerts,
Incidents, Devices, Administration. Unaffected by the backend's later
Go → Python rewrite — it only ever talked to the REST/WebSocket contract,
which stayed identical.

**Client-rendered by design, not server components everywhere.** Every
data-fetching page is a Client Component that calls the REST API directly
with the browser-held JWT — a deliberate simplification, since this app's
auth model (bearer tokens in `localStorage`, refreshed by the client)
doesn't map cleanly onto per-request server-side token forwarding.

**RBAC drives the UI, not just the API** — verified live by logging in as
each of two different roles and confirming the DOM actually differs: as
ADMIN, the Alerts page shows "Acknowledge" buttons and the Incidents detail
page shows an Actions panel; as VIEWER, those controls are simply absent
while the underlying data remains fully visible.

**The full incident lifecycle was exercised through the actual UI**:
clicking "Move to ACKNOWLEDGED" updated the status badge, removed the
now-invalid action buttons, and appended a real audit-history entry with a
live timestamp.

**`/ws/alerts` drives a live "● live" indicator and the Alerts table** —
acknowledging an alert updates its badge immediately via the REST call's
response, and the WebSocket connection is what the indicator reflects.

While building the Factory drill-down page, the frontend surfaced a real
API gap: there was no endpoint to list a production line's machines. Added
`GET /api/v1/production-lines/{id}/machines` to `services/api` rather than
working around it in the UI — the kind of thing you only find by actually
using what you built.

```bash
make up   # frontend included in the default stack
# http://localhost:3000 — demo login: admin@musterfabrik-gmbh.de / ChangeMe123!
```

**Not implemented here**: user creation/role-assignment UI (no backend
endpoint exists yet); a chart library beyond Recharts line charts
(no gauge/heatmap views); offline/optimistic UI for flaky connections.
