# Maintained EHS fork instructions

- Read `MAINTENANCE.md` before editing or releasing. `sunhaven/main` is the
  maintained release branch; `main` is an upstream mirror, not a deployment.
- Preserve `ehs_sentinel`, stable unique/entity IDs, legacy naming, original
  author attribution and upstream license. Never copy house configuration,
  credentials, .storage, account data, packet logs or FSV backups into this repo.
- Runtime source here is authoritative. Private HA repos may carry only an
  exact, pinned snapshot; never maintain conflicting edits in two repositories.
- Every write uses the existing producer and needs fresh current-connection
  evidence plus confirmed matching device state. Never introduce optimistic
  writes or weaken missing/stale/partial evidence checks to make tests pass.
- Catch ordinary polling errors within the cycle; propagate cancellation.
  Failed indoor reads must not starve outdoor health/power reads.
- Run both isolated test environments from `ehs-tests.yml`; never override a
  pytest-HA plugin's homeassistant pin. Tests must use fake transport/services.
- No force-push/rebase/history rewrite. Upstream changes enter a normal merge
  PR with regression tests; do not sync upstream over local safety commits.
- Releases are manual after exact-commit CI passes. Release version, manifest
  and device-info version must agree. Never move a published tag.
- Fork publishing does not authorize a HACS source switch, installation,
  restart, gate change, device command, FSV import or live canary.
