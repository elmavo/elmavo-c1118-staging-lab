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
  })
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
function mayPrepareOpening(plan, evidence, now) {
  return { allowed: !!evidence && evidence.githubAlreadyRunning === !0 && evidence.githubFreeQuotaVerified === !0 && evidence.githubRunId === plan.githubRunId && evidence.githubRunAttempt === plan.githubRunAttempt && evidence.githubSpendingBlocked === !0 && evidence.cloudFreeVerified === !0 && evidence.workflowArmed === !0 && evidence.workflowRunId === plan.runId && evidence.workflowDeadline === plan.expiresAt && evidence.credentialsApproved === !0 && evidence.lifecycleBudgetVerified === !0 && now >= plan.openedAt && now < plan.expiresAt - TWO_HOST.dataStopLeadMs, data: null, regionalD1: "BLOCKED", routeOpened: !1 };
}

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

// test/support/regionalRecoveryCollector.ts
function safeData(value, command) {
  if (!value || typeof value != "object") return !1;
  let v = value;
  return !(v.bookmark === null || typeof v.bookmark == "string" && /^[^\x00-\x20\x7f]{1,1024}$/.test(v.bookmark)) || !Number.isSafeInteger(v.revision) || ![0, 1].includes(Number(v.revision)) ? !1 : command.action !== "read" ? exactObject(value, ["operationId", "outcome", "revision", "bookmark"]) && value.operationId === command.operationId && ["applied", "conflict"].includes(String(value.outcome)) : exactObject(value, ["revision", "enabled", "bookmark", "meta"]) && value.enabled === 0 && exactObject(value.meta, ["served_by_region", "served_by_primary", "rows_read", "rows_written", "changes"]) && (value.meta.served_by_region === null || typeof value.meta.served_by_region == "string" && /^[A-Za-z0-9_-]{1,32}$/.test(value.meta.served_by_region)) && (value.meta.served_by_primary === null || typeof value.meta.served_by_primary == "boolean") && [value.meta.rows_read, value.meta.rows_written, value.meta.changes].every((n) => n === null || Number.isSafeInteger(n) && Number(n) >= 0);
}

// test/support/twoHostGithub.ts
async function runGithubHost(value, input, ports) {
  let count = 0, auditOK = !0, samples = [], no = () => ({ status: "DENY", samples: [], requests: count, regionalD1: "BLOCKED" });
  if (!ports) return no();
  let start = ports.clock.wall(), mono = ports.clock.monotonic(), now = () => Math.ceil(Math.max(ports.clock.wall(), start + ports.clock.monotonic() - mono)), plan, commands;
  try {
    if (plan = twoHostRun(value, now()), commands = recoveryCommands(input), commands.length > TWO_HOST.quotas["gh-data"] || !mayPrepareOpening(plan, await ports.readiness(plan), now()).allowed) return no();
  } catch {
    return no();
  }
  let record = (event, code) => {
    try {
      ports.audit.append({ event, code, at: now(), count }) || (auditOK = !1);
    } catch {
      auditOK = !1;
    }
  };
  if (record("armed", "ARMED"), !auditOK) return no();
  let used = { "gh-data": 0, "gh-close": 0, "gh-verify": 0 };
  async function send(role, request) {
    if (used[role] >= TWO_HOST.quotas[role] || (used[role]++, count++, record("request", "RESERVED"), !auditOK && role === "gh-data")) return null;
    try {
      return await ports.send(request);
    } catch {
      return null;
    }
  }
  let uncertain = !1;
  for (let command of commands) {
    if (!auditOK || now() >= plan.expiresAt - TWO_HOST.dataStopLeadMs) break;
    try {
      let response = await send("gh-data", { kind: "command", runId: plan.runId, command }), body = response && await boundedLocalJson(response);
      if (!response?.ok || !exactObject(body, ["allowed", "data"]) || body.allowed !== !0 || !safeData(body.data, command)) {
        uncertain = command.action === "cas";
        break;
      }
      if (now() >= plan.expiresAt - TWO_HOST.dataStopLeadMs) break;
      samples.push(body.data);
    } catch {
      uncertain = command.action === "cas";
      break;
    }
  }
  let wait = async (at) => {
    for (; now() < at; ) await ports.clock.sleep(Math.min(1e3, at - now()));
  };
  await wait(plan.expiresAt - TWO_HOST.githubCloseLeadMs);
  let closed = !1;
  for (let i = 0; i < 6; i++)
    try {
      let post = await send("gh-close", { kind: "disable" });
      if (!post?.ok || !closedSubdomain(await boundedLocalJson(post))) continue;
      let get = await send("gh-close", { kind: "state" });
      if (get?.ok && closedSubdomain(await boundedLocalJson(get))) {
        record("response", "API_OFF"), closed = !0;
        break;
      }
    } catch {
    }
  await wait(plan.expiresAt - TWO_HOST.verifyLeadMs);
  let verified = !1;
  for (let i = 0; i < 6; i++)
    try {
      let get = await send("gh-verify", { kind: "state" });
      if (!get?.ok || !closedSubdomain(await boundedLocalJson(get))) continue;
      if ((await send("gh-verify", { kind: "public" }))?.status === 404 && now() <= plan.expiresAt) {
        record("response", "API_OFF"), record("response", "ROUTE_OFF"), verified = !0;
        break;
      }
    } catch {
    }
  return record("finished", auditOK && closed && verified ? "CONFIRMED" : "FAILED"), {
    status: auditOK && closed && verified ? "CONFIRMED_GITHUB_CLOSE" : "BLOCKED_CLOSE_UNCONFIRMED",
    samples: auditOK ? samples : [],
    uncertain,
    requests: count,
    used,
    regionalD1: "BLOCKED"
  };
}
export {
  runGithubHost
};
