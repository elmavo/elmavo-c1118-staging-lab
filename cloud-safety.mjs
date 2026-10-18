// test/support/twoHostCloudWorker.mjs
import { DurableObject, WorkflowEntrypoint } from "cloudflare:workers";

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
function closedSubdomain(value) {
  if (!value || typeof value != "object" || Array.isArray(value)) return !1;
  let v = value, result = v.result;
  return Object.keys(v).every((k) => ["success", "result", "errors", "messages"].includes(k)) && v.success === !0 && (v.errors === void 0 || Array.isArray(v.errors) && v.errors.length === 0) && (v.messages === void 0 || Array.isArray(v.messages) && v.messages.length === 0) && !!result && typeof result == "object" && !Array.isArray(result) && Object.keys(result).sort().join(",") === "enabled,previews_enabled" && "enabled" in result && result.enabled === !1 && "previews_enabled" in result && result.previews_enabled === !1;
}

// test/support/twoHostSafetyCore.ts
var denied = () => ({ allowed: !1, code: "UNKNOWN", at: 0 });
function validLedger(value) {
  if (!exactObject(value, ["plan", "internalCount", "intents", "counts", "events"]) || !exactObject(value.counts, ["cf-close", "cf-verify"]) || !Number.isSafeInteger(value.internalCount) || Number(value.internalCount) < 1 || Number(value.internalCount) > TWO_HOST.quotas.internal || !Object.values(value.counts).every((n) => Number.isSafeInteger(n) && Number(n) >= 0 && Number(n) <= 12) || !value.intents || typeof value.intents != "object" || Array.isArray(value.intents) || Object.keys(value.intents).length > 24 || !Array.isArray(value.events) || value.events.length > 128) return !1;
  let counts = { "cf-close": 0, "cf-verify": 0 };
  for (let [key, intent] of Object.entries(value.intents)) {
    if (!exactObject(intent, ["actor", "kind", "outcome"]) || !["cf-close", "cf-verify"].includes(String(intent.actor)) || !["disable", "state", "public"].includes(String(intent.kind)) || !/^cf-(close|verify):([0-9]|1[01]):(disable|state|public)$/.test(key) || !key.startsWith(`${intent.actor}:`) || !key.endsWith(`:${intent.kind}`) || intent.outcome !== null && (!exactObject(intent.outcome, ["code", "at"]) || !["API_OFF", "ROUTE_OFF", "UNKNOWN"].includes(String(intent.outcome.code)) || !Number.isSafeInteger(intent.outcome.at))) return !1;
    counts[intent.actor]++;
  }
  return counts["cf-close"] === value.counts["cf-close"] && counts["cf-verify"] === value.counts["cf-verify"] && value.events.every((e) => exactObject(e, ["kind", "at", "ticket", "code"]) && ["ARM", "RPC", "RESERVE", "RESULT"].includes(String(e.kind)) && Number.isSafeInteger(e.at) && (e.ticket === null || typeof e.ticket == "string") && typeof e.code == "string" && /^[A-Z_]{1,32}$/.test(e.code));
}
function createTwoHostSafety(ports) {
  let receive = (state) => state.internalCount >= TWO_HOST.quotas.internal ? !1 : (state.internalCount++, state.events.push({ kind: "RPC", at: ports.now(), ticket: null, code: "RECEIVED" }), !0), same = (state, plan) => validLedger(state) && JSON.stringify(state.plan) === JSON.stringify(plan), validCollection = (v) => exactObject(v, ["runId", "used", "batches"]) && recoveryUuid(v.runId) && Number.isSafeInteger(v.used) && Number(v.used) >= 0 && Number(v.used) <= TWO_HOST.quotas["cf-data"] && !!v.batches && typeof v.batches == "object" && !Array.isArray(v.batches) && Object.keys(v.batches).length <= 6 && Object.entries(v.batches).every(([id, b]) => recoveryUuid(id) && exactObject(b, ["count", "done"]) && Number.isSafeInteger(b.count) && [1, 2].includes(Number(b.count)) && typeof b.done == "boolean") && Object.values(v.batches).reduce((n, b) => n + Number(b.count), 0) === v.used;
  async function arm(value) {
    if (!ports.enabled) return { armed: !1 };
    try {
      let plan = twoHostRun(value, ports.now(), !0);
      return await ports.storage.transaction(async (tx) => {
        let old = await tx.get("run");
        return old !== void 0 ? !validLedger(old) || JSON.stringify(old.plan) !== JSON.stringify(plan) || !receive(old) ? { armed: !1 } : (await tx.put("run", old), { armed: !0, runId: old.plan.runId, expiresAt: old.plan.expiresAt }) : plan.expiresAt <= ports.now() ? { armed: !1 } : (await tx.put("run", {
          plan,
          internalCount: 1,
          intents: {},
          counts: { "cf-close": 0, "cf-verify": 0 },
          events: [{ kind: "ARM", at: ports.now(), ticket: null, code: "ARMED" }]
        }), { armed: !0, runId: plan.runId, expiresAt: plan.expiresAt });
      });
    } catch {
      return { armed: !1 };
    }
  }
  async function attempt(value, actor, index, kind) {
    if (!ports.enabled || !["cf-close", "cf-verify"].includes(actor) || !Number.isSafeInteger(index) || index < 0 || index >= 12 || (actor === "cf-close" ? !["disable", "state"].includes(kind) : !["state", "public"].includes(kind))) return denied();
    let plan;
    try {
      plan = twoHostRun(value, ports.now(), !0);
    } catch {
      return denied();
    }
    let ticket = `${actor}:${index}:${kind}`;
    try {
      let reserved = await ports.storage.transaction(async (tx) => {
        let state = await tx.get("run");
        if (!validLedger(state) || JSON.stringify(state.plan) !== JSON.stringify(plan)) return { reserved: !1, previous: null };
        if (!receive(state)) return { reserved: !1, previous: null };
        await tx.put("run", state);
        let old = state.intents[ticket];
        if (old) return { reserved: !1, previous: old.outcome };
        let due = plan.expiresAt - (actor === "cf-close" ? TWO_HOST.cloudCloseLeadMs : TWO_HOST.verifyLeadMs);
        return ports.now() < due || state.counts[actor] >= TWO_HOST.quotas[actor] || Object.values(state.intents).filter((v) => v.outcome === null).length >= 2 ? { reserved: !1, previous: null } : (state.counts[actor]++, state.intents[ticket] = { actor, kind, outcome: null }, state.events.push({ kind: "RESERVE", at: ports.now(), ticket, code: "RESERVED" }), await tx.put("run", state), { reserved: !0, previous: null });
      });
      if (!reserved.reserved) return reserved.previous ? { allowed: !0, ...reserved.previous } : denied();
      let code = "UNKNOWN";
      try {
        let response = await ports.request(kind);
        kind === "public" ? response.status === 404 && (code = "ROUTE_OFF") : response.ok && closedSubdomain(await boundedLocalJson(response)) && (code = "API_OFF");
      } catch {
      }
      let outcome = { code, at: ports.now() };
      return await ports.storage.transaction(async (tx) => {
        let state = await tx.get("run");
        return !validLedger(state) || JSON.stringify(state.plan) !== JSON.stringify(plan) || !state.intents[ticket] || state.intents[ticket].outcome ? !1 : (state.intents[ticket].outcome = outcome, state.events.push({ kind: "RESULT", at: outcome.at, ticket, code }), await tx.put("run", state), !0);
      }) ? { allowed: !0, ...outcome } : denied();
    } catch {
      return denied();
    }
  }
  return {
    arm,
    attempt,
    async claimBatch(value, batchId, count) {
      if (!ports.enabled || !recoveryUuid(batchId) || ![1, 2].includes(count)) return !1;
      try {
        let plan = twoHostRun(value, ports.now());
        return await ports.storage.transaction(async (tx) => {
          let state = await tx.get("run");
          if (!same(state, plan) || !receive(state) || ports.now() >= plan.expiresAt - TWO_HOST.dataStopLeadMs) return !1;
          await tx.put("run", state);
          let old = await tx.get("collection");
          if (old !== void 0 && !validCollection(old)) return !1;
          let collection = old ?? { runId: plan.runId, used: 0, batches: {} };
          return collection.runId !== plan.runId || collection.batches[batchId] || Object.keys(collection.batches).length >= 6 || collection.used + count > TWO_HOST.quotas["cf-data"] || Object.values(collection.batches).some((b) => !b.done) ? !1 : (collection.used += count, collection.batches[batchId] = { count, done: !1 }, await tx.put("collection", collection), !0);
        });
      } catch {
        return !1;
      }
    },
    async collectionLive(value) {
      if (!ports.enabled) return !1;
      try {
        let plan = twoHostRun(value, ports.now());
        return await ports.storage.transaction(async (tx) => {
          let state = await tx.get("run");
          return !same(state, plan) || !receive(state) ? !1 : (await tx.put("run", state), ports.now() < plan.expiresAt - TWO_HOST.dataStopLeadMs);
        });
      } catch {
        return !1;
      }
    },
    async finishBatch(value, batchId) {
      if (!ports.enabled || !recoveryUuid(batchId)) return !1;
      try {
        let plan = twoHostRun(value, ports.now(), !0);
        return await ports.storage.transaction(async (tx) => {
          let c = await tx.get("collection"), state = await tx.get("run");
          return !same(state, plan) || !receive(state) || !validCollection(c) || !c.batches[batchId] || c.batches[batchId].done ? !1 : (await tx.put("run", state), c.batches[batchId].done = !0, await tx.put("collection", c), !0);
        });
      } catch {
        return !1;
      }
    }
  };
}
var once = { retries: { limit: 0, delay: 1e3, backoff: "constant" }, timeout: "5 seconds" };
async function runCloudDeadman(plan, steps, stub) {
  let armed = await steps.do("arm-public-plan-only", once, () => stub.arm(plan));
  if (!armed.armed || armed.runId !== plan.runId || armed.expiresAt !== plan.expiresAt) return { status: "DENY", regionalD1: "BLOCKED" };
  await steps.sleepUntil("independent-close", plan.expiresAt - TWO_HOST.cloudCloseLeadMs);
  let apiOff = !1;
  for (let i = 0; i < 6; i++) {
    let post = await steps.do(`close-${i}`, once, () => stub.attempt(plan, "cf-close", i * 2, "disable"));
    if (!post.allowed || post.code !== "API_OFF") continue;
    let check = await steps.do(`check-${i}`, once, () => stub.attempt(plan, "cf-close", i * 2 + 1, "state"));
    if (check.allowed && check.code === "API_OFF" && check.at <= plan.expiresAt) {
      apiOff = !0;
      break;
    }
  }
  await steps.sleepUntil("independent-verify", plan.expiresAt - TWO_HOST.verifyLeadMs);
  let observed = !1;
  for (let i = 0; i < 6; i++) {
    let check = await steps.do(`verify-api-${i}`, once, () => stub.attempt(plan, "cf-verify", i * 2, "state"));
    if (!check.allowed || check.code !== "API_OFF") continue;
    let https = await steps.do(`verify-https-${i}`, once, () => stub.attempt(plan, "cf-verify", i * 2 + 1, "public"));
    if (https.allowed && https.code === "ROUTE_OFF" && https.at <= plan.expiresAt) {
      observed = !0;
      break;
    }
  }
  return { status: apiOff && observed ? "CONFIRMED_CLOUD_CLOSE" : "BLOCKED_CLOSE_UNCONFIRMED", regionalD1: "BLOCKED" };
}

// test/support/twoHostCloudWorker.mjs
function enabled(env) {
  return env.C1118_SAFETY_ENABLED === "1" && env.SYNTHETIC_ONLY === "1" && env.ENTRY_ENABLED === "0" && env.ACCOUNT_ID === TWO_HOST.target.accountId && env.DATABASE_ID === TWO_HOST.target.databaseId && env.INSTALLATION_ID === TWO_HOST.target.installationId && typeof env.C1118_CLOSE_TOKEN == "string" && /^[A-Za-z0-9_-]{20,256}$/.test(env.C1118_CLOSE_TOKEN);
}
var C1118SafetyLedger = class extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env), this.safety = createTwoHostSafety({ storage: ctx.storage, enabled: enabled(env), now: () => Date.now(), request: async (kind) => {
      if (!enabled(env)) throw new Error("Close unavailable");
      let url = kind === "public" ? "https://staging.invalid/primary" : "https://api.cloudflare.com" + closeRoutePath;
      return fetch(url, {
        method: kind === "disable" ? "POST" : "GET",
        redirect: "error",
        cache: "no-store",
        signal: AbortSignal.timeout(1e3),
        headers: {
          "cache-control": "no-store",
          ...kind === "public" ? {} : { authorization: "Bearer " + env.C1118_CLOSE_TOKEN },
          ...kind === "disable" ? { "content-type": "application/json" } : {}
        },
        ...kind === "disable" ? { body: JSON.stringify({ enabled: !1, previews_enabled: !1 }) } : {}
      });
    } });
  }
  arm(plan) {
    return this.safety.arm(plan);
  }
  attempt(plan, actor, index, kind) {
    return this.safety.attempt(plan, actor, index, kind);
  }
  claimBatch(plan, id, count) {
    return this.safety.claimBatch(plan, id, count);
  }
  collectionLive(plan) {
    return this.safety.collectionLive(plan);
  }
  finishBatch(plan, id) {
    return this.safety.finishBatch(plan, id);
  }
  async fetch() {
    return Response.json({ allowed: !1, data: null }, { status: 403 });
  }
}, C1118Deadman = class extends WorkflowEntrypoint {
  async run(event, step) {
    if (!enabled(this.env)) return { status: "DENY", regionalD1: "BLOCKED" };
    try {
      let plan = twoHostRun(event.payload, Date.now(), !0);
      return this.env.RUN_ID !== plan.runId ? { status: "DENY", regionalD1: "BLOCKED" } : await runCloudDeadman(plan, step, this.env.SAFETY.getByName(plan.runId));
    } catch {
      return { status: "BLOCKED_CLOSE_UNCONFIRMED", regionalD1: "BLOCKED" };
    }
  }
}, twoHostCloudWorker_default = { async fetch() {
  return Response.json({ allowed: !1, data: null }, { status: 403, headers: { "cache-control": "no-store" } });
} };
export {
  C1118Deadman,
  C1118SafetyLedger,
  twoHostCloudWorker_default as default
};
