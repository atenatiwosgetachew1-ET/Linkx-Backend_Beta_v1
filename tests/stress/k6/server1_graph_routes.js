import http from 'k6/http';
import { check, sleep, fail } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://172.27.23.95:8000';
const LOGIN_PATH = __ENV.LOGIN_PATH || '/auth/login';
const INIT_PATH = __ENV.INIT_PATH || '/init';
const CONNECT_TOOL_PATH = __ENV.CONNECT_TOOL_PATH || '/connect_to_tool';
const GRAPH_LINK_PATH = __ENV.GRAPH_LINK_PATH || '/graph_link';
const GET_GRAPH_PATH = __ENV.GET_GRAPH_PATH || '/get_graph';
const USERNAME = __ENV.LINKX_USERNAME || '';
const PASSWORD = __ENV.LINKX_PASSWORD || '';
const THINK_TIME_MS = Number(__ENV.THINK_TIME_MS || '250');
const GRAPH_RELATIONSHIP = __ENV.GRAPH_RELATIONSHIP || 'TRANSACTS_TO';
const GRAPH_WINDOW_ID = __ENV.GRAPH_WINDOW_ID || 'k6_graph_window';
const ENABLE_UNLINK = (__ENV.ENABLE_UNLINK || 'false').toLowerCase() === 'true';
const NEO4J_URL = __ENV.NEO4J_URL || 'bolt://172.27.23.85:7687';
const NEO4J_USERNAME = __ENV.NEO4J_USERNAME || 'neo4j';
const NEO4J_PASSWORD = __ENV.NEO4J_PASSWORD || '';
const NEO4J_DATABASE = __ENV.NEO4J_DATABASE || '';

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

function connectTool(token, sessionId) {
  if (!NEO4J_PASSWORD) {
    fail('NEO4J_PASSWORD is required for server1_graph_routes.js');
  }

  const response = http.post(
    `${BASE_URL}${CONNECT_TOOL_PATH}`,
    JSON.stringify({
      tool_name: 'neo4j',
      url: NEO4J_URL,
      username: NEO4J_USERNAME,
      password: NEO4J_PASSWORD,
      database: NEO4J_DATABASE,
      session_id: sessionId,
      source_id: sessionId,
    }),
    {
      headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
      tags: { endpoint: 'connect_to_tool' },
    },
  );

  check(response, {
    'setup connect tool status is 200': (r) => r.status === 200,
    'setup connect tool succeeded': (r) => {
      try {
        return r.json('status') === 'success';
      } catch (_err) {
        return false;
      }
    },
  });

  if (response.status !== 200) {
    fail(`connect_to_tool failed with status ${response.status}`);
  }
  if (response.json('status') !== 'success') {
    fail(`connect_to_tool did not succeed: ${response.body}`);
  }
}

export function setup() {
  const token = loginOnce();
  const sessionId = initSession(token);
  connectTool(token, sessionId);
  return { token, sessionId };
}

function hitGraphLink(token, sessionId) {
  const response = http.post(
    `${BASE_URL}${GRAPH_LINK_PATH}`,
    JSON.stringify({
      id: 'link',
      session_id: sessionId,
      graph_window_id: GRAPH_WINDOW_ID,
    }),
    {
      headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
      tags: { endpoint: 'graph_link' },
    },
  );

  check(response, {
    'graph link status is 200': (r) => r.status === 200,
  });
}

function hitGetGraph(token, sessionId) {
  const response = http.post(
    `${BASE_URL}${GET_GRAPH_PATH}`,
    JSON.stringify({
      id: 'relationship',
      session_id: sessionId,
      relationship: GRAPH_RELATIONSHIP,
    }),
    {
      headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
      tags: { endpoint: 'get_graph' },
    },
  );

  check(response, {
    'get graph status is 200 or 202': (r) => r.status === 200 || r.status === 202,
    'get graph did not forbid access': (r) => r.status !== 401 && r.status !== 403,
  });
}

export default function (data) {
  const token = data.token;
  const sessionId = data.sessionId;

  hitGraphLink(token, sessionId);
  hitGetGraph(token, sessionId);

  if (ENABLE_UNLINK) {
    http.post(
      `${BASE_URL}${GRAPH_LINK_PATH}`,
      JSON.stringify({
        id: 'unlink',
        session_id: sessionId,
        graph_window_id: GRAPH_WINDOW_ID,
      }),
      {
        headers: jsonHeaders({ Authorization: `Bearer ${token}` }),
        tags: { endpoint: 'graph_unlink' },
      },
    );
  }

  sleep(THINK_TIME_MS / 1000);
}
