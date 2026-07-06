import http from 'k6/http';
import { check, sleep, fail } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://172.27.23.95:8000';
const LOGIN_PATH = __ENV.LOGIN_PATH || '/auth/login';
const INIT_PATH = __ENV.INIT_PATH || '/init';
const STR_PATH = __ENV.STR_PATH || '/api/STR_link_analysis';
const USERNAME = __ENV.LINKX_USERNAME || '';
const PASSWORD = __ENV.LINKX_PASSWORD || '';
const THINK_TIME_MS = Number(__ENV.THINK_TIME_MS || '250');
const STR_ENTITY = __ENV.STR_ENTITY || 'bank';
const STR_TYPE = __ENV.STR_TYPE || 'account_number';
const STR_VALUE = __ENV.STR_VALUE || '';
const STR_DATE = __ENV.STR_DATE || '';
const PUBLIC_API_KEY = __ENV.STR_PUBLIC_API_KEY || '';
const STR_ID_PREFIX = __ENV.STR_ID_PREFIX || 'k6_str';
const ACCEPT_NOT_FOUND = (__ENV.ACCEPT_NOT_FOUND || 'true').toLowerCase() === 'true';

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

function strBody(sessionId, iteration) {
  const body = {
    entity: STR_ENTITY,
    type: STR_TYPE,
    value: STR_VALUE || sessionId,
    session_id: sessionId,
    str_id: `${STR_ID_PREFIX}_${sessionId}_${iteration}`,
  };

  if (STR_DATE) {
    body.date = STR_DATE;
  }

  return body;
}

function hitStrAnalysis(token, sessionId, iteration) {
  const headers = { Authorization: `Bearer ${token}` };
  if (PUBLIC_API_KEY) {
    headers['X-API-Key'] = PUBLIC_API_KEY;
  }

  const response = http.post(
    `${BASE_URL}${STR_PATH}`,
    JSON.stringify(strBody(sessionId, iteration)),
    {
      headers: jsonHeaders(headers),
      tags: { endpoint: 'str_link_analysis' },
    },
  );

  const allowedOutcome = (r) => {
    if (r.status === 200 || r.status === 202) {
      try {
        const message = String(r.json('message') || '');
        return ACCEPT_NOT_FOUND ? true : message !== 'Not found!';
      } catch (_err) {
        return true;
      }
    }
    return false;
  };

  check(response, {
    'str route returned an allowed status': (r) => r.status === 200 || r.status === 202,
    'str route did not hard fail': allowedOutcome,
  });
}

export default function (data) {
  const token = data.token;
  const sessionId = data.sessionId;
  hitStrAnalysis(token, sessionId, __ITER);
  sleep(THINK_TIME_MS / 1000);
}
