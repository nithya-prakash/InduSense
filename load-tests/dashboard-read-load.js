// Simulates realistic concurrent dashboard usage: each virtual user is one
// logged-in operator polling the same endpoints the real Next.js dashboard
// calls — factories, devices, alerts, and a device's latest telemetry
// reading — at a human-realistic pace, not a tight request loop.
//
// Device/metric pairs are discovered from the running API in setup(), not
// hardcoded: this only works against demo data seeded by scripts/seed
// (`make seed`), and Postgres-generated UUIDs are different on every fresh
// seed, so a checked-in fixture file would go stale immediately.
//
// Each VU gets its own synthetic X-Forwarded-For identity, stable for that
// VU's whole run. This isn't working around the rate limiter — every VU's
// own request rate stays well under its 120 req/min allowance — it's
// modeling reality: the limiter is designed to give *each real client* a
// fair budget, and 50 concurrent browser tabs are 50 different clients,
// not one client making 50x the requests. Testing with everyone sharing
// one IP would measure the rate limiter's ceiling instead of the API's.
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8080';
const EMAIL = __ENV.LOAD_TEST_EMAIL || 'admin@musterfabrik-gmbh.de';
const PASSWORD = __ENV.LOAD_TEST_PASSWORD || 'ChangeMe123!';

export const options = {
  scenarios: {
    dashboard_read_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '20s', target: 50 },
        { duration: '40s', target: 50 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{endpoint:factories}': ['p(95)<300'],
    'http_req_duration{endpoint:devices}': ['p(95)<300'],
    'http_req_duration{endpoint:alerts}': ['p(95)<300'],
    'http_req_duration{endpoint:telemetry_latest}': ['p(95)<400'],
  },
};

function login() {
  const res = http.post(
    `${BASE_URL}/api/v1/auth/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  if (res.status !== 200) {
    throw new Error(`setup login failed: ${res.status} ${res.body}`);
  }
  return JSON.parse(res.body).access_token;
}

// Discovers real (device_id, metric) pairs by calling the actual API —
// GET /devices, then /devices/{id}/sensors for a sample of them — instead
// of querying Postgres directly, so this only ever exercises the public
// contract a real dashboard client also depends on.
function discoverDevicePairs(token) {
  const headers = { Authorization: `Bearer ${token}` };
  const devicesRes = http.get(`${BASE_URL}/api/v1/devices?limit=50`, { headers });
  if (devicesRes.status !== 200) {
    throw new Error(`setup device discovery failed: ${devicesRes.status} ${devicesRes.body}`);
  }
  const devices = JSON.parse(devicesRes.body).items;

  const pairs = [];
  for (const device of devices) {
    const sensorsRes = http.get(`${BASE_URL}/api/v1/devices/${device.id}/sensors`, { headers });
    if (sensorsRes.status !== 200) continue;
    const sensors = JSON.parse(sensorsRes.body).items;
    for (const sensor of sensors) {
      pairs.push({ device_id: device.id, metric: sensor.metric });
    }
  }
  if (pairs.length === 0) {
    throw new Error('setup found zero device/metric pairs — is the demo data seeded (`make seed`)?');
  }
  return pairs;
}

export function setup() {
  const token = login();
  const pairs = discoverDevicePairs(token);
  return { token, pairs };
}

export default function (data) {
  const clientIP = `10.load-test.${__VU}`; // stable per-VU identity for the whole run
  const headers = {
    Authorization: `Bearer ${data.token}`,
    'X-Forwarded-For': clientIP,
  };
  const pair = data.pairs[Math.floor(Math.random() * data.pairs.length)];

  let res = http.get(`${BASE_URL}/api/v1/factories?limit=20`, { headers, tags: { endpoint: 'factories' } });
  check(res, { 'factories: 200': (r) => r.status === 200 });

  res = http.get(`${BASE_URL}/api/v1/devices?limit=20`, { headers, tags: { endpoint: 'devices' } });
  check(res, { 'devices: 200': (r) => r.status === 200 });

  res = http.get(`${BASE_URL}/api/v1/alerts?limit=20`, { headers, tags: { endpoint: 'alerts' } });
  check(res, { 'alerts: 200': (r) => r.status === 200 });

  res = http.get(
    `${BASE_URL}/api/v1/telemetry/latest?device_id=${pair.device_id}&metric=${pair.metric}`,
    { headers, tags: { endpoint: 'telemetry_latest' } }
  );
  // 404 (no reading in the last 24h) is a legitimate, fast response for a
  // device the simulator hasn't touched recently — not a load-test failure.
  check(res, { 'telemetry: 200 or 404': (r) => r.status === 200 || r.status === 404 });

  sleep(1 + Math.random() * 2); // 1-3s between polls, roughly matching real dashboard auto-refresh
}
