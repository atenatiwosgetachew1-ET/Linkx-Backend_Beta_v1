import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8000';
const LOGIN_PATH = __ENV.LOGIN_PATH || '/auth/login';
const INIT_PATH = __ENV.INIT_PATH || '/init';
const HEALTH_PATH = __ENV.HEALTH_PATH || '/db/health';
const USERNAME = __ENV.LINKX_USERNAME || '';
const PASSWORD = __ENV.LINKX_PASSWORD || '';
const THINK_TIME_MS = Number(__ENV.THINK_TIME_MS || '250');
const ENABLE_INIT = (__ENV.ENABLE_INIT || 'true').toLowerCase() !== 'false';

const vus = Number(__ENV.K6_VUS || '10');
const duration = __ENV.K6_DURATION || '2m';

export const options = {
  vus,
  duration,
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<1500'],
  },
};

function jsonHeaders(extra = {}) {
  return Object.assign({ 'Content-Type': 'application/json' }, extra);
}

function maybeLogin() {
  if (!USERNAME || !PASSWORD) {
    return null;
  }

  const payload = JSON.stringify({
    username: USERNAME,
    password: PASSWORD,
  });
  const response = http.post(`${BASE_URL}${LOGIN_PATH}`, payload, {
    headers: jsonHeaders(),
    tags: { endpoint: 'auth_login' },
  });

  check(response, {
    'login status is 200': (r) => r.status === 200,
    'login returned token': (r) => {
      try {
        return Boolean(r.json('token'));
      } catch (_err) {
        return false;
      }
    },
  });

  if (response.status !== 200) {
    return null;
  }

  try {
    return response.json('token');
  } catch (_err) {
    return null;
  }
}

function hitHealth() {
  const response = http.get(`${BASE_URL}${HEALTH_PATH}`, {
    tags: { endpoint: 'db_health' },
  });
  check(response, {
    'health status is 200': (r) => r.status === 200,
  });
}

function hitInit(token) {
  if (!ENABLE_INIT || !token) {
    return;
  }

  const payload = JSON.stringify({ id: 'init' });
  const response = http.post(`${BASE_URL}${INIT_PATH}`, payload, {
    headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
    tags: { endpoint: 'init' },
  });

  check(response, {
    'init status is 200': (r) => r.status === 200,
    'init returned session id': (r) => {
      try {
        return Boolean(r.json('results.session_id'));
      } catch (_err) {
        return false;
      }
    },
  });
}

export default function () {
  hitHealth();

  const token = maybeLogin();
  hitInit(token);

  sleep(THINK_TIME_MS / 1000);
}
