import http from 'k6/http';
import { check, sleep, fail } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://172.27.23.95:8000';
const LOGIN_PATH = __ENV.LOGIN_PATH || '/auth/login';
const INIT_PATH = __ENV.INIT_PATH || '/init';
const ME_PATH = __ENV.ME_PATH || '/auth/me';
const VERIFY_PATH = __ENV.VERIFY_PATH || '/auth/verify';
const WORKSPACE_PATH = __ENV.WORKSPACE_PATH || '/workspace/layout';
const PREFERENCES_PATH = __ENV.PREFERENCES_PATH || '/auth/preferences';
const USERNAME = __ENV.LINKX_USERNAME || '';
const PASSWORD = __ENV.LINKX_PASSWORD || '';
const THINK_TIME_MS = Number(__ENV.THINK_TIME_MS || '200');
const ENABLE_WRITE = (__ENV.ENABLE_WRITE || 'false').toLowerCase() === 'true';

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

function loginOnce() {
  if (!USERNAME || !PASSWORD) {
    fail('LINKX_USERNAME and LINKX_PASSWORD are required');
  }

  const response = http.post(
    `${BASE_URL}${LOGIN_PATH}`,
    JSON.stringify({ username: USERNAME, password: PASSWORD }),
    { headers: jsonHeaders(), tags: { endpoint: 'auth_login' } },
  );

  check(response, {
    'setup login status is 200': (r) => r.status === 200,
    'setup login returned token': (r) => {
      try {
        return Boolean(r.json('token'));
      } catch (_err) {
        return false;
      }
    },
  });

  if (response.status !== 200) {
    fail(`login failed with status ${response.status}`);
  }

  return response.json('token');
}

function initSession(token) {
  const response = http.post(
    `${BASE_URL}${INIT_PATH}`,
    JSON.stringify({}),
    {
      headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
      tags: { endpoint: 'init' },
    },
  );

  check(response, {
    'setup init status is 200': (r) => r.status === 200,
    'setup init returned session id': (r) => {
      try {
        return Boolean(r.json('results.session_id'));
      } catch (_err) {
        return false;
      }
    },
  });

  if (response.status !== 200) {
    fail(`init failed with status ${response.status}`);
  }

  const sessionId = response.json('results.session_id');
  if (!sessionId) {
    fail('init did not return a session_id');
  }

  return sessionId;
}

export function setup() {
  const token = loginOnce();
  const sessionId = initSession(token);
  return { token, sessionId };
}

function hitMe(token) {
  const response = http.get(`${BASE_URL}${ME_PATH}`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { endpoint: 'auth_me' },
  });
  check(response, {
    'me status is 200': (r) => r.status === 200,
  });
}

function hitVerify(token) {
  const response = http.post(
    `${BASE_URL}${VERIFY_PATH}`,
    JSON.stringify({ token }),
    {
      headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
      tags: { endpoint: 'auth_verify' },
    },
  );
  check(response, {
    'verify status is 200': (r) => r.status === 200,
  });
}

function hitWorkspaceLayout(token, sessionId) {
  const getResponse = http.get(`${BASE_URL}${WORKSPACE_PATH}?session_id=${encodeURIComponent(sessionId)}`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { endpoint: 'workspace_layout_get' },
  });

  check(getResponse, {
    'workspace layout get is 200': (r) => r.status === 200,
  });

  if (ENABLE_WRITE) {
    const putResponse = http.put(
      `${BASE_URL}${WORKSPACE_PATH}`,
      JSON.stringify({
        session_id: sessionId,
        layout: {
          active_panel: 'summary',
          panels: ['summary', 'graph'],
          viewport: { columns: 2, rows: 1 },
        },
      }),
      {
        headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
        tags: { endpoint: 'workspace_layout_put' },
      },
    );

    check(putResponse, {
      'workspace layout put is 200': (r) => r.status === 200,
    });
  }
}

function hitPreferences(token) {
  const getResponse = http.get(`${BASE_URL}${PREFERENCES_PATH}`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { endpoint: 'preferences_get' },
  });

  check(getResponse, {
    'preferences get is 200': (r) => r.status === 200,
  });

  if (ENABLE_WRITE) {
    const patchResponse = http.patch(
      `${BASE_URL}${PREFERENCES_PATH}`,
      JSON.stringify({
        preferences: {
          remember_layout: true,
          enable_notifications: false,
        },
      }),
      {
        headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
        tags: { endpoint: 'preferences_patch' },
      },
    );

    check(patchResponse, {
      'preferences patch is 200': (r) => r.status === 200,
    });
  }
}

export default function (data) {
  const token = data.token;
  const sessionId = data.sessionId;

  hitMe(token);
  hitVerify(token);
  hitWorkspaceLayout(token, sessionId);
  hitPreferences(token);

  sleep(THINK_TIME_MS / 1000);
}
