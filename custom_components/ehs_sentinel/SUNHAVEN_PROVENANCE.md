# Maintained EHS Sentinel fork

Source of this component: `madboymatt26/ehs_sentinel_hacs_integration`, maintained
branch `sunhaven/main`. Fork release: **2.1.3**.
Upstream merge baseline: `echoDaveD/ehs_sentinel_hacs_integration` tag
`v2.1.2`, commit `95d14284c61561a167062c053c3f88c7161b051c`, by echoDaveD.
The fork's original baseline was
v2.0.2 (`af4783c8aaae8b0be80cdc8a1cea0b73f973fe32`).

The adjacent LICENSE reproduces upstream's abbreviated notice verbatim.
Upstream attribution, domain and stable naming/unique IDs are preserved.
Local patches add source freshness, strict readback, bounded transport and
resilient polling; they are not claimed to exist in upstream 2.0.2. Version
2.0.4 also marks periodic freshness publication as an event-loop callback;
2.0.3 failed real interval dispatch and must not be deployed.
See the fork's MAINTENANCE.md and immutable release for provenance/tests.

Version 2.1.3 merges upstream known-entity/polling/fault improvements
with migration safeguards preserving legacy identities and registry entries.
It retains 2.0.5's live receipt clocks/unrecorded metadata and five measurement
statistics sensors. No installation or restart is part of this merge.

No house configuration, credentials, account storage, packet logs or FSV
backups are included. Publication is not physical commissioning or permission
to send commands, import FSVs, change write mode or restart an installation.
