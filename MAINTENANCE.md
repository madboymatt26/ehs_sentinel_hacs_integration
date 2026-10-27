# Maintenance, updates and HACS migration

## Source and release policy

Upstream baseline: `echoDaveD/ehs_sentinel_hacs_integration` tag `v2.0.2`,
commit `af4783c8aaae8b0be80cdc8a1cea0b73f973fe32`. The source snapshot was
compared with that baseline; unchanged files differ only in line endings.
The original abbreviated upstream LICENSE is retained verbatim.

`sunhaven/main` is the reviewed source of fork releases. `main` remains an
upstream mirror. Version 2.0.3 adds receipt-based availability, strict command
readback, transport/request/publication safety and recovery of periodic reads.
It has no house configuration, secret material or live diagnostics.

Version 2.0.4 corrects a live-discovered 2.0.3 interval-dispatch defect: the
synchronous freshness publisher must be marked as a Home Assistant callback,
otherwise the timer runs it in an executor thread. The regression now uses
the real HA interval scheduler, verifies repeated event-loop publication and
expiry, and verifies cancellation on unload. Do not deploy 2.0.3: its direct-call
tests missed the timer dispatch error. No device command or policy is changed.

There is deliberately no automatic semantic-release workflow. Create a PR,
pass both isolated HA 2026.6.1 and HA 2026.9.4 regression environments, review
and merge, then tag the exact tested commit `v<manifest version>`. Publish a
GitHub release explicitly identifying it as a fork release and naming upstream
baseline and tests. Never move a release tag or auto-deploy a moving branch.

## Taking upstream updates

The 2.1.3 candidate normally merges upstream v2.1.2 (95d1428), including
2.1.0 known-entity/polling improvements and 2.1.1/2.1.2 fault handling. It retains
the fork's receipt-correlated acknowledgements, event-loop freshness timer and
recorder/statistics changes. Existing entries missing `only_known_entities`
default to False; new setup defaults to True. Filtering is opt-in for upgrades,
does not delete registry entries and never re-enables user-disabled entities.
First valid values enable only integration-disabled entries. The legacy
`NASA_ERROR_CODE1` indoor identity stays; `NASA_ERROR_CODE1_OUTDOOR` is additive,
with source-class routing before freshness validation. Invalid `ffff` outdoor
payloads cannot refresh availability, publish a fault or acknowledge a request.
Malformed nested polling configuration is rejected before starting a poll loop.
This candidate requires dual-version CI and review before release/installation.

Version 2.0.5 retains per-register receipt clocks live, but declares only
`nasa_last_seen` unrecorded using Home Assistant's supported entity metadata.
This allows recorder attribute deduplication while keeping actual values,
availability and source-freshness failures in history. It does not suppress
state publication or claim that every unchanged-state row disappears. Five
read-only flow/return/outdoor/cylinder-temperature and instantaneous-power
sensors now declare measurement statistics, with unchanged IDs and units.
No command, polling interval, freshness budget or write prerequisite changes.

1. Fetch upstream into the upstream mirror, keeping maintained history intact.
2. Create an update branch from `sunhaven/main` and merge the desired upstream
   commit normally. Resolve conflicts without dropping freshness/write checks.
3. Run both test environments; add regressions for changed APIs and verify
   stable IDs/naming and HACS metadata. Review any new dependency versions.
4. Review/merge, assign the next fork version and publish an immutable tag.
5. Update the consuming private HA project's version/commit/tree/checksum lock
   and exact source snapshot in a review PR. Installation remains a separately
   approved maintenance action, never a side effect of upstream syncing.

Generic fixes can be submitted upstream as separate focused PRs. No automatic
messages/PRs to the upstream maintainer are created by this maintenance setup.

## HACS migration boundary

HACS replaces files; it does not merge patches or update a configuration repo.
Changing versions of the original repository does not retain local edits.
Register this fork as a custom Integration repository and use its approved tag
only after verifying the installed HACS version's supported source-replacement
flow. Both repositories share `ehs_sentinel`, so never install both concurrently.
Do not remove the actual HA integration/config entry or entity registry.

Before changing code/source: back up the exact old component and supported
configuration, record IDs/options/enable states, exclude the original EHS
repository from any automatic updater and arrange a maintenance window.
For controllers consuming new freshness attributes, hold their approved writers
while installing coordinated guard/code changes. Restart only with approval.
Validate loaded version, current receipts, guards, entity IDs and zero
unexpected actions before restoring the saved writer enable states.
Observe natural confirmed writes; never induce a physical command as a test.
Keep the previous pinned release/backup as the rollback target.

An update badge/ignored release is not an access-control boundary. Source
ownership, approved immutable releases and consumer checksum checks provide
the repeatable process; a drift check should fail, not auto-repair live files.
