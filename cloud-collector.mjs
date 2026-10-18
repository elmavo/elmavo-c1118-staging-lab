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

// test/support/cloudSafetyProtocol.ts
function recoveryCommands(value) {
  if (!Array.isArray(value) || value.length > 70) throw new Error("Collector commands refused");
  for (let c of value)
    if (!(exactObject(c, ["action", "operationId", "expectedRevision"]) && c.action === "cas" && recoveryUuid(c.operationId) && c.expectedRevision === 0) && !(exactObject(c, ["action", "operationId"]) && c.action === "receipt" && recoveryUuid(c.operationId)) && !(exactObject(c, ["action", "mode", "bookmark"]) && c.action === "read" && (c.mode === "bookmark" && typeof c.bookmark == "string" && /^[^\x00-\x20\x7f]{1,1024}$/.test(c.bookmark) || ["primary", "unconstrained"].includes(String(c.mode)) && c.bookmark === null)))
      throw new Error("Collector commands refused");
  return value;
}

// test/support/regionalRecoveryCollector.ts
function safeData(value, command) {
  if (!value || typeof value != "object") return !1;
  let v = value;
  return !(v.bookmark === null || typeof v.bookmark == "string" && /^[^\x00-\x20\x7f]{1,1024}$/.test(v.bookmark)) || !Number.isSafeInteger(v.revision) || ![0, 1].includes(Number(v.revision)) ? !1 : command.action !== "read" ? exactObject(value, ["operationId", "outcome", "revision", "bookmark"]) && value.operationId === command.operationId && ["applied", "conflict"].includes(String(value.outcome)) : exactObject(value, ["revision", "enabled", "bookmark", "meta"]) && value.enabled === 0 && exactObject(value.meta, ["served_by_region", "served_by_primary", "rows_read", "rows_written", "changes"]) && (value.meta.served_by_region === null || typeof value.meta.served_by_region == "string" && /^[A-Za-z0-9_-]{1,32}$/.test(value.meta.served_by_region)) && (value.meta.served_by_primary === null || typeof value.meta.served_by_primary == "boolean") && [value.meta.rows_read, value.meta.rows_written, value.meta.changes].every((n) => n === null || Number.isSafeInteger(n) && Number(n) >= 0);
}

// test/support/stagingProbeProtocol.ts
async function stagingProbeAuthenticated(request, env) {
  let token = request.headers.get("authorization")?.match(/^Bearer ([a-f0-9]{64})$/)?.[1];
  if (!token || !env.OPERATOR_KEY || !/^[a-f0-9]{64}$/.test(env.OPERATOR_KEY)) return !1;
  let algorithm = { name: "HMAC", hash: "SHA-256" }, encoder = new TextEncoder(), message = encoder.encode("ELMAVO C11.12 provider-only"), key = await crypto.subtle.importKey("raw", encoder.encode(env.OPERATOR_KEY), algorithm, !1, ["sign"]), signature = await crypto.subtle.sign("HMAC", key, message), candidate = await crypto.subtle.importKey("raw", encoder.encode(token), algorithm, !1, ["verify"]);
  return crypto.subtle.verify("HMAC", candidate, signature, message);
}
async function stagingProbeBody(request) {
  if (request.headers.get("content-type") !== "application/json" || !request.body) return null;
  let reader = request.body.getReader(), length = 0, chunks = [];
  try {
    for (; ; ) {
      let part = await reader.read();
      if (part.done) break;
      if (length += part.value.byteLength, length > 2048)
        return await reader.cancel(), null;
      chunks.push(part.value);
    }
    let bytes = new Uint8Array(length), offset = 0;
    for (let chunk of chunks)
      bytes.set(chunk, offset), offset += chunk.length;
    let parsed = JSON.parse(new TextDecoder("utf-8", { fatal: !0 }).decode(bytes));
    return parsed !== null && typeof parsed == "object" && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

// test/support/twoHostCollector.ts
async function twoHostCollector(request, env, ports) {
  let no = () => Response.json({ allowed: !1, data: null }, { status: 403, headers: { "cache-control": "no-store" } }), claimed = !1, batch = "", result = no();
  try {
    let url = new URL(request.url);
    if (!ports || env.C1118_COLLECTOR_ENABLED !== "1" || env.ENTRY_ENABLED !== "0" || env.SYNTHETIC_ONLY !== "1" || env.ACCOUNT_ID !== TWO_HOST.target.accountId || env.DATABASE_ID !== TWO_HOST.target.databaseId || env.INSTALLATION_ID !== TWO_HOST.target.installationId || !recoveryUuid(env.RUN_ID) || url.protocol !== "https:" || url.pathname !== "/__c1118/collect" || url.search || request.method !== "POST" || !await stagingProbeAuthenticated(request, { DB: void 0, OPERATOR_KEY: env.OPERATOR_KEY })) return no();
    let body = await stagingProbeBody(request);
    if (!exactObject(body, ["runId", "batchId", "commands"]) || body.runId !== env.RUN_ID || !recoveryUuid(body.batchId)) return no();
    let commands = recoveryCommands(body.commands);
    if (commands.length < 1 || commands.length > 2 || !await ports.claim(body.batchId, commands.length)) return no();
    claimed = !0, batch = body.batchId;
    let samples = [];
    for (let command of commands) {
      if (!await ports.live()) throw new Error("Unavailable");
      let start = performance.now(), response = await ports.send(env.RUN_ID, command), value = await boundedLocalJson(response);
      if (!response.ok || !exactObject(value, ["allowed", "data"]) || value.allowed !== !0 || !safeData(value.data, command)) throw new Error("Unavailable");
      samples.push({ data: value.data, elapsedMs: performance.now() - start });
    }
    await ports.live() && (result = Response.json({ allowed: !0, data: {
      samples,
      actualExecutionRegionConfirmed: !1,
      regionalD1: "BLOCKED_REQUIRES_REMOTE_ARTIFACT_AUDIT"
    } }, { headers: { "cache-control": "no-store" } }));
  } catch {
    result = no();
  } finally {
    if (claimed) try {
      (!await ports.finish(batch) || !await ports.live()) && (result = no());
    } catch {
      result = no();
    }
  }
  return result;
}

// test/support/twoHostCollectorWorker.mjs
var twoHostCollectorWorker_default = { async fetch(request, env) {
  if (env.C1118_COLLECTOR_ENABLED !== "1") return twoHostCollector(request, env);
  try {
    let plan = twoHostRun(JSON.parse(env.PUBLIC_RUN_PLAN), Date.now());
    if (plan.runId !== env.RUN_ID) return twoHostCollector(request, env);
    let stub = env.SAFETY.getByName(plan.runId);
    return await twoHostCollector(request, env, {
      claim: (id, count) => stub.claimBatch(plan, id, count),
      live: () => stub.collectionLive(plan),
      finish: (id) => stub.finishBatch(plan, id),
      send: (runId, command) => fetch(
        "https://staging.invalid/__c1116/runs/" + runId + "/command",
        {
          method: "POST",
          headers: { "content-type": "application/json", authorization: "Bearer " + env.OPERATOR_KEY },
          body: JSON.stringify(command),
          redirect: "error",
          cache: "no-store",
          signal: AbortSignal.timeout(1e3)
        }
      )
    });
  } catch {
    return Response.json({ allowed: !1, data: null }, { status: 403 });
  }
} };
export {
  twoHostCollectorWorker_default as default
};
