// test/support/regionalRecoveryIdentity.ts
var STAGING_INSTALLATION = "00000000-0000-4000-8000-000000000002", STAGING_DATABASE = "00000000-0000-4000-8000-000000000001", RECOVERY_TARGET = Object.freeze({
  accountId: "00000000000000000000000000000000",
  databaseId: STAGING_DATABASE,
  installationId: STAGING_INSTALLATION,
  worker: "example-synthetic-probe",
  normalRequests: 170,
  cleanupRequests: 30,
  normalConcurrent: 3,
  concurrent: 4
});

// test/support/regionalRecoveryNode.ts
var closeRoutePath = `/client/v4/accounts/${RECOVERY_TARGET.accountId}/workers/scripts/${RECOVERY_TARGET.worker}/subdomain`;

// test/support/twoHostSafetyPlan.ts
var TWO_HOST = Object.freeze({
  target: RECOVERY_TARGET,
  githubRunner: "ubuntu-24.04",
  collector: "elmavo-stg-c1118-collector",
  safety: "elmavo-stg-c1118-safety",
  workflow: "elmavo-stg-c1118-deadman",
  placementHint: "aws:eu-central-1",
  maxWindowMs: 6e5,
  dataStopLeadMs: 18e4,
  cloudCloseLeadMs: 12e4,
  githubCloseLeadMs: 9e4,
  verifyLeadMs: 4e4,
  // 84 provider calls + 64 internal RPC allowance + 52 lifecycle = 200.
  quotas: Object.freeze({
    "gh-data": 24,
    "cf-data": 12,
    "gh-close": 12,
    "cf-close": 12,
    "gh-verify": 12,
    "cf-verify": 12,
    internal: 64,
    lifecycle: 52
  })
});

// test/support/twoHostPreparation.ts
process.stdout.write(JSON.stringify({
  version: "elmavo.c1118.prepare.v1",
  host1: TWO_HOST.githubRunner,
  host2: TWO_HOST.collector,
  cloudSafety: TWO_HOST.workflow,
  quotas: TWO_HOST.quotas,
  requests: 0,
  entryEnabled: !1,
  githubActivated: !1,
  cloudActivated: !1,
  externalCredentialsBridgeReady: !1,
  executionRegionConfirmed: !1,
  regionalD1: "BLOCKED"
}) + `
`);
