// Complementary to dashboard-read-load.js, and deliberately the opposite
// design: this one *doesn't* spread identities across VUs. It proves the
// login endpoint's per-IP rate limiter (default 10/min, from
// API_RATE_LIMIT_AUTH_PER_MIN) actually holds under a real k6-generated
// burst from a single client, not just a hand-rolled curl loop — a
// security property worth its own load test, separate from the capacity
// question dashboard-read-load.js answers.
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    auth_burst: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 30, // well over the 10/min limit from one shared identity
      maxDuration: '30s',
    },
  },
  thresholds: {
    // The real assertion: at least one request must be rejected. A run
    // where 100% succeed means the limiter isn't enforcing at all.
    'checks{check:got_429}': ['rate>0'],
  },
};

const CLIENT_IP = '203.0.113.99'; // one shared synthetic identity for every VU/iteration in this test

export default function () {
  const res = http.post(
    `${BASE_URL}/api/v1/auth/login`,
    JSON.stringify({ email: 'rate-limit-load-test@musterfabrik-gmbh.de', password: 'wrong-on-purpose' }),
    {
      headers: { 'Content-Type': 'application/json', 'X-Forwarded-For': CLIENT_IP },
    }
  );
  check(res, {
    'got 401 or 429': (r) => r.status === 401 || r.status === 429,
    got_429: (r) => r.status === 429,
  });
}
