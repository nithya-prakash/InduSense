// Connection-scale test for /ws/alerts, the real-time fan-out built in
// Phase 10. dashboard-read-load.js tests the request/response side of the
// API; this tests the other half — how many concurrent long-lived
// WebSocket connections a single api replica can hold open and keep
// broadcasting to, since that's a fundamentally different resource profile
// (one goroutine + one open TCP connection per client, not a
// request/response cycle).
//
// This measures connection scale, not delivery — it doesn't assert every
// connection receives a specific alert (that would require also firing
// the simulator in lockstep, adding a second moving part to a scale test
// that's really about "can 200 sockets stay open and healthy"). Message
// receipt was already proven for real in Phase 11's live browser
// verification of the "● live" indicator.
import ws from 'k6/ws';
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8080';
const WS_URL = BASE_URL.replace(/^http/, 'ws');
const EMAIL = __ENV.LOAD_TEST_EMAIL || 'admin@musterfabrik-gmbh.de';
const PASSWORD = __ENV.LOAD_TEST_PASSWORD || 'ChangeMe123!';
const HOLD_OPEN_SECONDS = 20;

export const options = {
  scenarios: {
    websocket_scale: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 200 },
        { duration: `${HOLD_OPEN_SECONDS}s`, target: 200 },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    'checks{check:connected}': ['rate>0.99'],
    'checks{check:clean_close}': ['rate>0.99'],
  },
};

export function setup() {
  const res = http.post(
    `${BASE_URL}/api/v1/auth/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  if (res.status !== 200) {
    throw new Error(`setup login failed: ${res.status} ${res.body}`);
  }
  return { token: JSON.parse(res.body).access_token };
}

export default function (data) {
  const url = `${WS_URL}/ws/alerts?token=${data.token}`;
  let connected = false;
  let cleanClose = false;

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', () => {
      connected = true;
      socket.setTimeout(() => socket.close(), HOLD_OPEN_SECONDS * 1000);
    });
    socket.on('error', () => {
      cleanClose = false;
    });
    socket.on('close', () => {
      cleanClose = connected; // only "clean" if we actually got past open first
    });
  });

  check(res, {
    connected: () => connected,
    clean_close: () => cleanClose,
  });
}
