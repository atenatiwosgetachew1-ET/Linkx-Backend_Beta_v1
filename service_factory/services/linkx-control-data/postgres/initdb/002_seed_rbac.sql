-- Baseline RBAC seed data. Application bootstrap code may add/update these too.

INSERT INTO roles (name, description) VALUES
    ('superuser', 'Full system access'),
    ('admin', 'Administrative analyst access'),
    ('analyst', 'Standard analyst access'),
    ('viewer', 'Read-only access')
ON CONFLICT (name) DO NOTHING;

INSERT INTO permissions (key) VALUES
    ('superuser:manage'),
    ('users:manage'),
    ('auth:verify'),
    ('config:read'),
    ('config:write'),
    ('source:create'),
    ('source:connect'),
    ('source:disconnect'),
    ('graph:create'),
    ('graph:read'),
    ('graph:link'),
    ('batch:upload'),
    ('batch:query'),
    ('analysis:run'),
    ('reports:read'),
    ('session:create'),
    ('session:read')
ON CONFLICT (key) DO NOTHING;

INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM roles r
JOIN permissions p ON (
    r.name = 'superuser'
    OR (r.name = 'admin' AND p.key IN (
        'users:manage','config:read','config:write','source:create','source:connect','source:disconnect',
        'graph:create','graph:read','graph:link','batch:upload','batch:query','analysis:run',
        'reports:read','session:create','session:read'
    ))
    OR (r.name = 'analyst' AND p.key IN (
        'config:read','source:create','source:connect','source:disconnect','graph:create','graph:read',
        'graph:link','batch:upload','batch:query','analysis:run','reports:read','session:create','session:read'
    ))
    OR (r.name = 'viewer' AND p.key IN ('config:read','graph:read','reports:read','session:read'))
)
ON CONFLICT DO NOTHING;
