// test/support/closedGithubJobProcess.ts
import { appendFileSync } from "node:fs";
import { platform } from "node:os";

// test/support/regionalRecoveryIdentity.ts
var STAGING_INSTALLATION = "7edbcbcb-53d5-4313-9b21-79ebf46e93b2", STAGING_DATABASE = "028a0748-e72f-4e61-94a7-3f51789785d9", RECOVERY_TARGET = Object.freeze({
  accountId: "d02455ab29c87ddbce551ab741e10df4",
  databaseId: STAGING_DATABASE,
  installationId: STAGING_INSTALLATION,
  worker: "elmavo-stg-c1110-7edbcbcb53d5",
  normalRequests: 170,
  cleanupRequests: 30,
  normalConcurrent: 3,
  concurrent: 4
}), recoveryUuid = (v) => typeof v == "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(v);

// test/support/regionalRecoveryNode.ts
var exactObject = (v, keys) => !!v && typeof v == "object" && !Array.isArray(v) && Object.keys(v).sort().join(",") === keys.sort().join(",");
async function boundedLocalJson(response) {
  let reader = response.body?.getReader();
  if (!reader) throw new Error("Local recovery response refused");
  let chunks = [], size = 0;
  for (; ; ) {
    let part = await reader.read();
    if (part.done) break;
    if (size += part.value.length, size > 32768)
      throw await reader.cancel(), new Error("Local recovery response refused");
    chunks.push(part.value);
  }
  if (response.headers.get("content-type")?.split(";")[0] !== "application/json") throw new Error("Local recovery response refused");
  let joined = new Uint8Array(size), offset = 0;
  for (let c of chunks)
    joined.set(c, offset), offset += c.length;
  return JSON.parse(new TextDecoder("utf-8", { fatal: !0 }).decode(joined));
}
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
  }),
  // Non-safety RPCs stop at 40, leaving the existing 24 close/verify slots.
  // This partitions the existing 200-request budget; it adds no requests.
  nonSafetyInternalLimit: 40
});
function twoHostRun(value, now, cleanup = !1) {
  if (!exactObject(value, ["version", "runId", "githubRunId", "githubRunAttempt", "accountId", "databaseId", "installationId", "worker", "openedAt", "expiresAt", "resourceApprovedAt", "resourceExpiresAt"]) || value.version !== "elmavo.c1118.run.v1" || !recoveryUuid(value.runId) || typeof value.githubRunId != "string" || !/^[0-9]{1,20}$/.test(value.githubRunId) || !Number.isSafeInteger(value.githubRunAttempt) || Number(value.githubRunAttempt) < 1 || Number(value.githubRunAttempt) > 100 || value.accountId !== RECOVERY_TARGET.accountId || value.databaseId !== RECOVERY_TARGET.databaseId || value.installationId !== RECOVERY_TARGET.installationId || value.worker !== RECOVERY_TARGET.worker || ![now, value.openedAt, value.expiresAt, value.resourceApprovedAt, value.resourceExpiresAt].every(Number.isSafeInteger) || Number(value.resourceApprovedAt) > now || Number(value.openedAt) < Number(value.resourceApprovedAt) || Number(value.openedAt) > now + 6e4 || Number(value.expiresAt) - Number(value.openedAt) < 18e4 || Number(value.expiresAt) - Number(value.openedAt) > TWO_HOST.maxWindowMs || Number(value.resourceExpiresAt) - Number(value.resourceApprovedAt) > 864e5 || Number(value.expiresAt) > Number(value.resourceExpiresAt) || !cleanup && (Number(value.expiresAt) <= now || Number(value.resourceExpiresAt) <= now))
    throw new Error("Two-host plan refused");
  return Object.freeze({
    version: "elmavo.c1118.run.v1",
    runId: value.runId,
    githubRunId: value.githubRunId,
    githubRunAttempt: Number(value.githubRunAttempt),
    accountId: value.accountId,
    databaseId: value.databaseId,
    installationId: value.installationId,
    worker: value.worker,
    openedAt: Number(value.openedAt),
    expiresAt: Number(value.expiresAt),
    resourceApprovedAt: Number(value.resourceApprovedAt),
    resourceExpiresAt: Number(value.resourceExpiresAt)
  });
}

// test/support/closedGithubJob.ts
import { mkdirSync, lstatSync as lstatSync2, openSync as openSync2, fsyncSync as fsyncSync2, closeSync as closeSync2, constants as constants2 } from "node:fs";
import { join as join2, isAbsolute } from "node:path";

// test/support/cloudSafetyProtocol.ts
function recoveryCommands(value) {
  if (!Array.isArray(value) || value.length > 70) throw new Error("Collector commands refused");
  for (let c of value)
    if (!(exactObject(c, ["action", "operationId", "expectedRevision"]) && c.action === "cas" && recoveryUuid(c.operationId) && c.expectedRevision === 0) && !(exactObject(c, ["action", "operationId"]) && c.action === "receipt" && recoveryUuid(c.operationId)) && !(exactObject(c, ["action", "mode", "bookmark"]) && c.action === "read" && (c.mode === "bookmark" && typeof c.bookmark == "string" && /^[^\x00-\x20\x7f]{1,1024}$/.test(c.bookmark) || ["primary", "unconstrained"].includes(String(c.mode)) && c.bookmark === null)))
      throw new Error("Collector commands refused");
  return value;
}
function closedSubdomain(value) {
  if (!value || typeof value != "object" || Array.isArray(value)) return !1;
  let v = value, result = v.result;
  return Object.keys(v).every((k) => ["success", "result", "errors", "messages"].includes(k)) && v.success === !0 && (v.errors === void 0 || Array.isArray(v.errors) && v.errors.length === 0) && (v.messages === void 0 || Array.isArray(v.messages) && v.messages.length === 0) && !!result && typeof result == "object" && !Array.isArray(result) && Object.keys(result).sort().join(",") === "enabled,previews_enabled" && "enabled" in result && result.enabled === !1 && "previews_enabled" in result && result.previews_enabled === !1;
}

// test/support/closedKillSwitchObserver.ts
async function runClosedKillSwitchObserver(value, ports) {
  let count = 0, auditOK = !0, result = (status) => ({
    status,
    requests: count,
    auditDurable: auditOK,
    data: null,
    cloudExecutionVerified: !1,
    routeTransitionVerified: !1,
    regionalD1: "BLOCKED"
  });
  if (!ports) return result("DENY");
  try {
    let start = ports.clock.wall(), mono = ports.clock.monotonic(), now = () => Math.ceil(Math.max(ports.clock.wall(), start + ports.clock.monotonic() - mono)), plan = twoHostRun(value, now());
    if (plan.expiresAt - plan.openedAt !== 18e4 || !await ports.authorize(plan)) return result("DENY");
    let record = (event, code, httpStatus) => {
      try {
        ports.audit.append({ event, code, at: now(), count, ...httpStatus === void 0 ? {} : { httpStatus } }) || (auditOK = !1);
      } catch {
        auditOK = !1;
      }
      return auditOK;
    };
    if (!record("armed", "ARMED")) return result("DENY");
    let verifyAt = plan.expiresAt - TWO_HOST.verifyLeadMs;
    for (; now() < verifyAt; ) await ports.clock.sleep(Math.min(1e3, verifyAt - now()));
    let call = async (kind) => {
      if (!auditOK || now() >= plan.expiresAt || count >= TWO_HOST.quotas["gh-verify"] || (count++, !record("request", "RESERVED")) || now() >= plan.expiresAt) return null;
      try {
        return await ports.request(kind);
      } catch {
        return null;
      }
    };
    for (; count + 2 <= TWO_HOST.quotas["gh-verify"] && now() < plan.expiresAt && auditOK; ) {
      let state = await call("state");
      if (state?.tlsVerified && state.response.status === 200 && closedSubdomain(await boundedLocalJson(state.response)) && now() < plan.expiresAt) {
        if (!record("response", "API_OFF", 200)) break;
        let publicCheck = await call("public");
        if (publicCheck?.tlsVerified && publicCheck.response.status === 404 && now() <= plan.expiresAt) {
          if (!record("response", "ROUTE_OFF", 404)) break;
          return record("finished", "CONFIRMED") && now() <= plan.expiresAt ? result("CLOSED_RESPONSE_OBSERVED") : (record("finished", "FAILED"), result("BLOCKED"));
        }
      }
      now() < plan.expiresAt && auditOK && await ports.clock.sleep(Math.min(1e3, plan.expiresAt - now()));
    }
    return record("finished", "FAILED"), result("BLOCKED");
  } catch {
    return result("BLOCKED");
  }
}

// test/support/cloudSafetyJournal.ts
import { createHash } from "node:crypto";
import { closeSync, constants, fsyncSync, lstatSync, openSync, writeSync } from "node:fs";
import { join } from "node:path";

// test/support/cloudSafetyPlan.ts
var CLOUD_SAFETY = Object.freeze({
  target: RECOVERY_TARGET,
  origin: "https://elmavo-stg-c1110-7edbcbcb53d5.elmavo-stg-7edbcbcb53d5.workers.dev",
  api: "https://api.cloudflare.com",
  quota: { "collector-1": 70, "collector-2": 70, controller: 24, observer: 20, lifecycle: 16 },
  closeLeadMs: 9e4,
  observeLeadMs: 4e4,
  maxWindowMs: 6e5,
  maxLifetimeMs: 864e5
});
var sha256Text = (v) => typeof v == "string" && /^[a-f0-9]{64}$/.test(v), cloudRole = (v) => typeof v == "string" && ["collector-1", "collector-2", "controller", "observer"].includes(v);

// test/support/cloudSafetyJournal.ts
var planDigest = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
function createSafetyJournal(directory, planSha256, role, runId) {
  if (!sha256Text(planSha256) || !cloudRole(role) || !recoveryUuid(runId)) throw new Error("Journal refused");
  let stat = lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 63) !== 0)
    throw new Error("Private journal directory required");
  let path = join(directory, `${runId}.${role}.jsonl`), fd = openSync(path, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 384), dir = openSync(directory, constants.O_RDONLY);
  try {
    fsyncSync(dir);
  } catch (error) {
    throw closeSync(fd), error;
  } finally {
    closeSync(dir);
  }
  let sequence = 0, previous = "0".repeat(64), closed = !1, broken = !1;
  return {
    path,
    append(value) {
      if (closed || broken || Object.keys(value).some((k) => ![
        "event",
        "at",
        "count",
        "code",
        "httpStatus",
        "revision",
        "bookmarkHash",
        "region",
        "primary",
        "operationId",
        "mode",
        "bookmarkInputHash",
        "elapsedMs",
        "rowsRead",
        "rowsWritten"
      ].includes(k)) || !["armed", "request", "response", "finished"].includes(value.event) || !["ARMED", "RESERVED", "DENIED", "UNKNOWN", "API_OFF", "ROUTE_OFF", "SAMPLE", "CONFIRMED", "FAILED"].includes(value.code) || !Number.isSafeInteger(value.at) || !Number.isSafeInteger(value.count) || value.count < 0 || value.count > 70 || value.httpStatus !== void 0 && (!Number.isSafeInteger(value.httpStatus) || value.httpStatus < 100 || value.httpStatus > 599) || value.revision !== void 0 && ![0, 1].includes(value.revision) || value.bookmarkHash !== void 0 && !sha256Text(value.bookmarkHash) || value.region !== void 0 && value.region !== null && !/^[A-Za-z0-9_-]{1,32}$/.test(value.region) || value.primary !== void 0 && value.primary !== null && typeof value.primary != "boolean" || value.operationId !== void 0 && !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.operationId) || value.mode !== void 0 && !["primary", "bookmark", "unconstrained"].includes(value.mode) || value.bookmarkInputHash !== void 0 && !sha256Text(value.bookmarkInputHash) || value.elapsedMs !== void 0 && (!Number.isFinite(value.elapsedMs) || value.elapsedMs < 0) || [value.rowsRead, value.rowsWritten].some((v) => v !== void 0 && (!Number.isSafeInteger(v) || v < 0))) return !1;
      try {
        let record = { version: "elmavo.c1117.audit.v1", runId, planSha256, role, sequence: ++sequence, previous, ...value }, hash2 = planDigest(record), bytes = Buffer.from(JSON.stringify({ ...record, hash: hash2 }) + `
`), offset = 0;
        for (; offset < bytes.length; ) {
          let written = writeSync(fd, bytes, offset, bytes.length - offset);
          if (written <= 0) throw new Error("Audit unavailable");
          offset += written;
        }
        return fsyncSync(fd), previous = hash2, !0;
      } catch {
        return broken = !0, !1;
      }
    },
    close() {
      closed || (closed = !0, closeSync(fd));
    }
  };
}

// test/support/closedSafetyEvidence.ts
var CLOSED_INSPECTIONS = Object.freeze(["safety-state", "safety-settings", "workflow-definition"]), object = (v) => !!v && typeof v == "object" && !Array.isArray(v);
function closedSafetyEvidence(kind, value, plan) {
  if (!object(value) || value.success !== !0 || !Array.isArray(value.errors) || value.errors.length || value.messages !== void 0 && (!Array.isArray(value.messages) || value.messages.length) || !object(value.result)) return !1;
  if (kind === "safety-state") return closedSubdomain(value);
  let r = value.result;
  if (kind === "workflow-definition") return r.id === "8b984ed9-ea26-4b1b-8cb1-070c87564819" && r.name === TWO_HOST.workflow && r.script_name === TWO_HOST.safety && r.class_name === "C1118Deadman" && (r.script_deleted === void 0 || r.script_deleted === !1) && (r.schedules === void 0 || Array.isArray(r.schedules) && r.schedules.length === 0);
  if (kind !== "safety-settings" || !Array.isArray(r.bindings) || r.bindings.length !== 11) return !1;
  let plain = {
    ENTRY_ENABLED: "0",
    SYNTHETIC_ONLY: "1",
    C1118_SAFETY_ENABLED: "1",
    ACCOUNT_ID: plan.accountId,
    DATABASE_ID: plan.databaseId,
    INSTALLATION_ID: plan.installationId,
    RUN_ID: plan.runId
  }, seen = /* @__PURE__ */ new Set();
  for (let b of r.bindings) {
    if (!object(b) || typeof b.name != "string" || seen.has(b.name)) return !1;
    if (seen.add(b.name), Object.hasOwn(plain, b.name)) {
      if (!exactObject(b, ["name", "type", "text"]) || b.type !== "plain_text" || b.text !== plain[b.name]) return !1;
    } else if (b.name === "C1118_CLOSE_TOKEN") {
      if (!exactObject(b, ["name", "type"]) || b.type !== "secret_text") return !1;
    } else if (b.name === "SAFETY") {
      if (!Object.keys(b).every((k) => ["name", "type", "namespace_id", "class_name", "script_name", "environment"].includes(k)) || b.type !== "durable_object_namespace" || b.namespace_id !== "08499e2430cb4fe582922cfaa473d505" || b.class_name !== void 0 && b.class_name !== "C1118SafetyLedger" || b.script_name !== void 0 && b.script_name !== TWO_HOST.safety || b.environment !== void 0 && b.environment !== "production") return !1;
    } else if (b.name === "COLLECTOR") {
      if (!Object.keys(b).every((k) => ["name", "type", "service", "environment"].includes(k)) || b.type !== "service" || b.service !== TWO_HOST.collector || b.environment !== void 0 && b.environment !== "production") return !1;
    } else if (b.name === "DEADMAN") {
      if (!Object.keys(b).every((k) => ["name", "type", "workflow_name", "class_name", "script_name"].includes(k)) || b.type !== "workflow" || b.workflow_name !== TWO_HOST.workflow || b.class_name !== void 0 && b.class_name !== "C1118Deadman" || b.script_name !== void 0 && b.script_name !== TWO_HOST.safety) return !1;
    } else return !1;
  }
  return Object.keys(plain).every((k) => seen.has(k)) && ["C1118_CLOSE_TOKEN", "SAFETY", "COLLECTOR", "DEADMAN"].every((k) => seen.has(k));
}

// test/support/cloudKillSwitchExercise.ts
function independentClosedRun(value, now, cleanup = !1) {
  let plan = twoHostRun(value, now, cleanup);
  if (plan.githubRunId !== "0" || plan.githubRunAttempt !== 1 || plan.expiresAt - plan.openedAt !== 18e4 || plan.resourceExpiresAt - plan.resourceApprovedAt !== 9e5)
    throw new Error("Independent closed plan refused");
  return plan;
}

// test/support/closedGithubJob.ts
var hash = (v) => typeof v == "string" && /^[a-f0-9]{40}$/.test(v);
function context(value) {
  return exactObject(value, ["repository", "runId", "runAttempt", "eventName", "actions", "runnerOs", "sha"]) && value.repository === "elmavo/elmavo-c1118-staging-lab" && value.actions === !0 && value.eventName === "workflow_dispatch" && value.runnerOs === "Linux" && typeof value.runId == "string" && /^[1-9][0-9]{0,19}$/.test(value.runId) && value.runAttempt === 1 && hash(value.sha);
}
function claimClosedGithubJournal(root, value, now) {
  let plan = twoHostRun(value, now);
  if (typeof root != "string" || !isAbsolute(root)) throw new Error("Private audit root required");
  let stat = lstatSync2(root);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("Private audit root required");
  let directory = join2(root, `elmavo-c1123-${plan.runId}`);
  mkdirSync(directory, { mode: 448 });
  let parent = openSync2(root, constants2.O_RDONLY);
  try {
    fsyncSync2(parent);
  } finally {
    closeSync2(parent);
  }
  return { directory, journal: createSafetyJournal(directory, planDigest(plan), "observer", plan.runId) };
}
function prepareClosedGithubPlan(value, gh, now) {
  if (obj(value) && value.version === "elmavo.c1131.job-input.v3") {
    if (!context(gh) || !exactObject(value, ["version", "plan", "planSha256", "expectedVersionId", "approvedCommitSha", "approved"]) || value.approved !== !0 || value.approvedCommitSha !== gh.sha || !recoveryUuid(value.expectedVersionId))
      throw new Error("Independent observer input refused");
    let plan2 = independentClosedRun(value.plan, now, !0);
    if (value.planSha256 !== planDigest(plan2)) throw new Error("Independent plan digest refused");
    return {
      plan: plan2,
      expectedVersionId: value.expectedVersionId,
      approvedCommitSha: gh.sha,
      planSha256: planDigest(plan2),
      observerMode: "independent-closed-only"
    };
  }
  let fixed = obj(value) && value.version === "elmavo.c1125.job-input.v2";
  if (!context(gh) || !exactObject(value, [
    "version",
    "runId",
    "resourceApprovedAt",
    "resourceExpiresAt",
    "expectedVersionId",
    "approvedCommitSha",
    "approved",
    ...fixed ? ["openedAt", "expiresAt"] : []
  ]) || !fixed && value.version !== "elmavo.c1123.job-input.v1" || value.approved !== !0 || !recoveryUuid(value.runId) || !recoveryUuid(value.expectedVersionId) || value.approvedCommitSha !== gh.sha || !Number.isSafeInteger(now) || !Number.isSafeInteger(value.resourceApprovedAt) || !Number.isSafeInteger(value.resourceExpiresAt) || Number(value.resourceApprovedAt) > now || Number(value.resourceExpiresAt) - Number(value.resourceApprovedAt) !== 9e5 || now + 6e4 > Number(value.resourceApprovedAt) + 3e5 || now + 36e4 > Number(value.resourceExpiresAt) || fixed && (!Number.isSafeInteger(value.openedAt) || !Number.isSafeInteger(value.expiresAt) || Number(value.openedAt) <= now || Number(value.expiresAt) - Number(value.openedAt) !== 18e4)) throw new Error("Closed job input refused");
  let plan = twoHostRun({
    version: "elmavo.c1118.run.v1",
    runId: value.runId,
    githubRunId: gh.runId,
    githubRunAttempt: gh.runAttempt,
    accountId: TWO_HOST.target.accountId,
    databaseId: TWO_HOST.target.databaseId,
    installationId: TWO_HOST.target.installationId,
    worker: TWO_HOST.target.worker,
    resourceApprovedAt: value.resourceApprovedAt,
    resourceExpiresAt: value.resourceExpiresAt,
    openedAt: fixed ? value.openedAt : now + 6e4,
    expiresAt: fixed ? value.expiresAt : now + 24e4
  }, now);
  return {
    plan,
    expectedVersionId: value.expectedVersionId,
    approvedCommitSha: gh.sha,
    planSha256: planDigest(plan),
    observerMode: "legacy"
  };
}
var obj = (v) => !!v && typeof v == "object" && !Array.isArray(v), decoded = (v) => typeof v == "string" && v.length <= 16384 ? JSON.parse(v) : v;
function timestamp(v) {
  return typeof v != "string" || !/^\d{4}-\d{2}-\d{2}T.*Z$/.test(v) ? NaN : Date.parse(v);
}
function closedWorkflowEvidence(value, plan, version, now) {
  try {
    if (!Number.isSafeInteger(now) || now > plan.expiresAt || !obj(value) || value.success !== !0 || !Array.isArray(value.errors) || value.errors.length || !obj(value.result)) return "DENY";
    let r = value.result;
    if (r.versionId !== version || !["queued", "running", "waiting", "complete"].includes(String(r.status)) || JSON.stringify(twoHostRun(decoded(r.params), now, !0)) !== JSON.stringify(plan) || !Array.isArray(r.steps) || r.steps.length > 64) return "DENY";
    let arms = r.steps.filter((s) => obj(s) && s.name === "arm-public-plan-only");
    if (!arms.length) return "WAIT";
    if (arms.length !== 1 || !obj(arms[0])) return "DENY";
    let arm = arms[0], output = decoded(arm.output);
    if (arm.type !== "step" || arm.success !== !0 || !exactObject(output, ["armed", "runId", "expiresAt"]) || output.armed !== !0 || output.runId !== plan.runId || output.expiresAt !== plan.expiresAt || !Number.isFinite(timestamp(arm.start)) || timestamp(arm.start) < plan.resourceApprovedAt || timestamp(arm.end) < timestamp(arm.start) || timestamp(arm.end) > now || !Number.isFinite(timestamp(arm.end)) || timestamp(arm.end) > plan.expiresAt - TWO_HOST.cloudCloseLeadMs) return "DENY";
    if (r.status !== "complete") return "ARMED";
    let result = decoded(r.output), end = timestamp(r.end);
    return !exactObject(result, ["status", "regionalD1"]) || result.status !== "CONFIRMED_CLOUD_CLOSE" || result.regionalD1 !== "BLOCKED" || !Number.isFinite(end) || end < timestamp(arm.end) || end > now || end > plan.expiresAt ? "DENY" : "COMPLETE";
  } catch {
    return "DENY";
  }
}
async function runClosedGithubJob(planValue, version, gh, ports, mode = "legacy") {
  let metadataRequests = 0, observerRequests = 0, inspectionRequests = 0, auditOK = !0, result = (status) => ({
    status,
    metadataRequests,
    observerRequests,
    inspectionRequests,
    requests: metadataRequests + observerRequests + inspectionRequests,
    auditDurable: auditOK,
    data: null,
    cloudExecutionVerified: !1,
    routeTransitionVerified: !1,
    regionalD1: "BLOCKED"
  });
  if (!ports || !ports.credentialAvailable || typeof ports.inspect != "function" || !context(gh) || !recoveryUuid(version) || !["legacy", "independent-closed-only"].includes(mode)) return result("DENY");
  try {
    let start = ports.clock.wall(), mono = ports.clock.monotonic(), now = () => Math.ceil(Math.max(ports.clock.wall(), start + ports.clock.monotonic() - mono)), independent = mode === "independent-closed-only", plan = independent ? independentClosedRun(planValue, now()) : twoHostRun(planValue, now());
    if (!independent && (plan.githubRunId !== gh.runId || plan.githubRunAttempt !== gh.runAttempt) || plan.expiresAt - plan.openedAt !== 18e4 || plan.resourceExpiresAt - plan.resourceApprovedAt !== 9e5)
      return result("DENY");
    let audit = { append(e) {
      try {
        ports.audit.append(e) || (auditOK = !1);
      } catch {
        auditOK = !1;
      }
      return auditOK;
    } };
    if (!audit.append({ event: "armed", code: "ARMED", at: now(), count: 0 })) return result("DENY");
    let finish = (status) => {
      let timely = now() <= plan.expiresAt;
      return audit.append({
        event: "finished",
        code: timely && status === "EVIDENCE_CONSISTENT" ? "CONFIRMED" : "FAILED",
        at: now(),
        count: metadataRequests + observerRequests + inspectionRequests
      }), timely && status === "EVIDENCE_CONSISTENT" && now() > plan.expiresAt && audit.append({ event: "finished", code: "FAILED", at: now(), count: metadataRequests + observerRequests + inspectionRequests }), result(auditOK && timely && now() <= plan.expiresAt ? status : "BLOCKED");
    }, inspect = async () => {
      for (let kind of CLOSED_INSPECTIONS) {
        if (!auditOK || now() >= plan.expiresAt || inspectionRequests >= 6 || (inspectionRequests++, !audit.append({ event: "request", code: "RESERVED", at: now(), count: metadataRequests + observerRequests + inspectionRequests })) || now() >= plan.expiresAt) return !1;
        try {
          let q = await ports.inspect(kind);
          if (!q.tlsVerified || q.response.status !== 200 || now() > plan.expiresAt) return !1;
          let body = await boundedLocalJson(q.response);
          if (now() > plan.expiresAt || !closedSafetyEvidence(kind, body, plan)) return !1;
        } catch {
          return !1;
        }
      }
      return !0;
    };
    if (!await inspect()) return finish("BLOCKED");
    ports.ready(plan);
    let metadata = async () => {
      if (!auditOK || now() >= plan.expiresAt || metadataRequests >= 8 || (metadataRequests++, !audit.append({ event: "request", code: "RESERVED", at: now(), count: metadataRequests + observerRequests + inspectionRequests })) || now() >= plan.expiresAt) return "DENY";
      try {
        let q = await ports.workflow();
        return !q.tlsVerified || now() > plan.expiresAt ? "DENY" : q.response.status === 404 ? "WAIT" : q.response.status !== 200 ? "DENY" : closedWorkflowEvidence(await boundedLocalJson(q.response), plan, version, now());
      } catch {
        return "WAIT";
      }
    }, armed = !1, armEvidenceUntil = independent ? plan.expiresAt : plan.expiresAt - TWO_HOST.cloudCloseLeadMs;
    for (let i = 0; i < 4 && now() < armEvidenceUntil; i++) {
      let state = await metadata();
      if (state === "DENY") return finish("BLOCKED");
      if (state === "ARMED" || state === "COMPLETE") {
        armed = !0;
        break;
      }
      await ports.clock.sleep(Math.min(3e4, Math.max(0, armEvidenceUntil - now())));
    }
    if (!armed) return finish("BLOCKED");
    let observed = await runClosedKillSwitchObserver(plan, {
      authorize: async () => armed,
      clock: { wall: now, monotonic: () => ports.clock.monotonic(), sleep: (ms) => ports.clock.sleep(ms) },
      audit: { append: (e) => audit.append({ ...e, count: metadataRequests + inspectionRequests + e.count }) },
      request: ports.observe
    });
    if (observerRequests = observed.requests, observed.auditDurable || (auditOK = !1), observed.status !== "CLOSED_RESPONSE_OBSERVED" || !auditOK) return finish("BLOCKED");
    for (let i = 0; i < 4 && now() < plan.expiresAt; i++) {
      let state = await metadata();
      if (state === "COMPLETE") return finish(await inspect() ? "EVIDENCE_CONSISTENT" : "BLOCKED");
      if (state === "DENY") break;
      await ports.clock.sleep(Math.min(5e3, Math.max(0, plan.expiresAt - now())));
    }
    return finish("BLOCKED");
  } catch {
    return result("BLOCKED");
  }
}

// test/support/cloudSafetyTransport.ts
import { request as httpsRequest } from "node:https";
import { checkServerIdentity } from "node:tls";
function safetyHttps(url, credential, body) {
  return new Promise((resolve, reject) => {
    let fail = () => reject(new Error("Staging transport unavailable")), req = httpsRequest(url, {
      method: body ? "POST" : "GET",
      agent: !1,
      rejectUnauthorized: !0,
      checkServerIdentity,
      minVersion: "TLSv1.2",
      headers: {
        "cache-control": "no-store",
        ...body ? { "content-type": "application/json" } : {},
        ...credential ? { authorization: `Bearer ${credential}` } : {}
      }
    }, (res) => {
      let size = 0, parts = [];
      res.on("data", (chunk) => {
        size += chunk.length, size > 32768 ? (res.destroy(), req.destroy(), fail()) : parts.push(chunk);
      }), res.on("error", fail), res.on("end", () => {
        clearTimeout(timer);
        let status = res.statusCode ?? 500;
        if (status >= 300 && status < 400) {
          fail();
          return;
        }
        try {
          resolve(new Response([204, 205].includes(status) ? null : Buffer.concat(parts), {
            status,
            headers: { "content-type": String(res.headers["content-type"] ?? "") }
          }));
        } catch {
          fail();
        }
      });
    }), timer = setTimeout(() => {
      req.destroy(), fail();
    }, 1e3);
    req.on("error", () => {
      clearTimeout(timer), fail();
    }), req.end(body);
  });
}
function strictWorkflowObserverTransport(credential, instanceId) {
  if (!/^[A-Za-z0-9_-]{20,256}$/.test(credential) || !recoveryUuid(instanceId))
    throw new Error("Workflow observer refused");
  return () => safetyHttps(`${CLOUD_SAFETY.api}/client/v4/accounts/${CLOUD_SAFETY.target.accountId}/workflows/elmavo-stg-c1118-deadman/instances/${instanceId}`, credential);
}
function strictClosedInspectionTransport(credential) {
  if (!/^[A-Za-z0-9_-]{20,256}$/.test(credential)) throw new Error("Closed inspection refused");
  let base = `${CLOUD_SAFETY.api}/client/v4/accounts/${CLOUD_SAFETY.target.accountId}`, paths = {
    "safety-state": "/workers/scripts/elmavo-stg-c1118-safety/subdomain",
    "safety-settings": "/workers/scripts/elmavo-stg-c1118-safety/settings",
    "workflow-definition": "/workflows/elmavo-stg-c1118-deadman"
  };
  return (kind) => {
    if (!CLOSED_INSPECTIONS.includes(kind)) throw new Error("Closed inspection refused");
    return safetyHttps(base + paths[kind], credential);
  };
}
function strictCloudSafetyTransport(role, credential) {
  if (!cloudRole(role) || typeof credential != "string" || (role.startsWith("collector") ? !/^[a-f0-9]{64}$/.test(credential) : !/^[A-Za-z0-9_-]{20,256}$/.test(credential)))
    throw new Error("Staging credential refused");
  return async (input) => {
    if (!(input.kind === "command" ? exactObject(input, ["kind", "runId", "command"]) : exactObject(input, ["kind"]) && ["disable", "state", "public"].includes(input.kind)))
      throw new Error("Staging operation refused");
    input.kind === "command" && recoveryCommands([input.command]);
    let collector = role.startsWith("collector");
    if (input.kind === "command" && (!collector || !recoveryUuid(input.runId)) || input.kind === "disable" && role !== "controller" || input.kind === "state" && collector || input.kind === "public" && role !== "observer")
      throw new Error("Staging operation refused");
    let url = input.kind === "command" ? CLOUD_SAFETY.origin + `/__c1116/runs/${input.runId}/command` : input.kind === "public" ? CLOUD_SAFETY.origin + "/primary" : CLOUD_SAFETY.api + closeRoutePath, body = input.kind === "disable" ? JSON.stringify({ enabled: !1, previews_enabled: !1 }) : input.kind === "command" ? JSON.stringify(input.command) : void 0;
    return safetyHttps(url, input.kind === "public" ? void 0 : credential, body);
  };
}

// test/support/closedGithubJobProcess.ts
async function main() {
  if (process.argv.length === 2 || process.argv.length === 3 && process.argv[2] === "--prepare") {
    process.stdout.write(JSON.stringify({
      version: "elmavo.c1125.job-preparation.v1",
      requests: 0,
      githubActivated: !1,
      cloudActivated: !1,
      entryEnabled: "0",
      regionalD1: "BLOCKED"
    }) + `
`);
    return;
  }
  if (process.argv.length !== 3 || process.argv[2] !== "--run-approved" || platform() !== "linux" || process.env.ELMAVO_C1123_ENABLE !== "1") throw new Error("Closed job refused");
  let credential = process.env.ELMAVO_C1123_READ_TOKEN, input = process.env.ELMAVO_C1123_INPUT;
  if (delete process.env.ELMAVO_C1123_READ_TOKEN, delete process.env.ELMAVO_C1123_INPUT, !credential || !/^[A-Za-z0-9_-]{20,256}$/.test(credential) || !input || Buffer.byteLength(input) > 8192)
    throw new Error("Closed job input refused");
  let gh = {
    repository: process.env.GITHUB_REPOSITORY ?? "",
    runId: process.env.GITHUB_RUN_ID ?? "",
    runAttempt: Number(process.env.GITHUB_RUN_ATTEMPT),
    eventName: process.env.GITHUB_EVENT_NAME ?? "",
    actions: process.env.GITHUB_ACTIONS === "true",
    runnerOs: process.env.RUNNER_OS ?? "",
    sha: process.env.GITHUB_SHA ?? ""
  }, draft = JSON.parse(input);
  if (!draft || typeof draft != "object" || !("version" in draft) || !["elmavo.c1125.job-input.v2", "elmavo.c1131.job-input.v3"].includes(String(draft.version)))
    throw new Error("Fixed deadline required");
  let prepared = prepareClosedGithubPlan(draft, gh, Date.now());
  if (prepared.observerMode === "independent-closed-only" && Date.now() >= prepared.plan.expiresAt) {
    process.stdout.write(JSON.stringify({
      status: "BLOCKED",
      code: "DEADLINE_MISSED",
      requests: 0,
      data: null,
      cloudExecutionVerified: !1,
      routeTransitionVerified: !1,
      regionalD1: "BLOCKED"
    }) + `
`), process.exitCode = 1;
    return;
  }
  let root = process.env.RUNNER_TEMP;
  if (!root) throw new Error("Private audit root required");
  let { directory, journal } = claimClosedGithubJournal(root, prepared.plan, Date.now()), read = strictCloudSafetyTransport("observer", credential), metadata = strictWorkflowObserverTransport(credential, prepared.plan.runId), inspection = strictClosedInspectionTransport(credential);
  try {
    process.env.GITHUB_OUTPUT && appendFileSync(process.env.GITHUB_OUTPUT, `audit-directory=${directory}
`);
    let result = await runClosedGithubJob(prepared.plan, prepared.expectedVersionId, gh, {
      credentialAvailable: !0,
      audit: journal,
      clock: { wall: () => Date.now(), monotonic: () => performance.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)) },
      ready(plan) {
        process.stdout.write(JSON.stringify({
          status: prepared.observerMode === "independent-closed-only" ? "OBSERVER_ATTACHED_NOT_A_SAFETY_GATE" : "JOB_READY_NOT_CLOUD_ARMED",
          plan,
          observerExecution: { runId: gh.runId, attempt: gh.runAttempt, sha: gh.sha },
          expectedVersionId: prepared.expectedVersionId,
          planSha256: prepared.planSha256
        }) + `
`);
      },
      workflow: async () => ({ response: await metadata(), tlsVerified: !0 }),
      inspect: async (kind) => ({ response: await inspection(kind), tlsVerified: !0 }),
      observe: async (kind) => ({ response: await read({ kind }), tlsVerified: !0 })
    }, prepared.observerMode);
    process.stdout.write(JSON.stringify(result) + `
`), result.status !== "EVIDENCE_CONSISTENT" && (process.exitCode = 1);
  } finally {
    journal.close();
  }
}
process.stdout.on("error", () => {
});
main().catch(() => {
  process.stdout.write(JSON.stringify({ status: "DENY", data: null, cloudExecutionVerified: !1, regionalD1: "BLOCKED" }) + `
`), process.exitCode = 1;
});
