import { describe, expect, it } from "vitest";
import type { ConnectorCtx, ConnectorEvent } from "transit-connector-kit";
import mattermost, { decomposeConversation, normalizePost } from "../src";

const SERVER = "https://mm.example.com";

type Route = { method: string; path: string; body?: string };

type Harness = {
  ctx: ConnectorCtx;
  routes: Route[];
  ingested: ConnectorEvent[];
  storage: Map<string, unknown>;
  respond(method: string, path: string, body: unknown, status?: number, headers?: Record<string, string>): void;
};

function harness(overrides: Record<string, string> = {}): Harness {
  const durable = new Map<string, unknown>();
  const runtime = new Map<string, unknown>();
  const routes: Route[] = [];
  const ingested: ConnectorEvent[] = [];
  const responses = new Map<
    string,
    { body: unknown; status: number; headers: Record<string, string> }
  >();

  const ctx: ConnectorCtx = {
    config: {
      server_url: SERVER,
      bot_token: "token",
      ...overrides,
    },
    storage: {
      get: async <T>(key: string) => durable.get(key) as T | undefined,
      put: async (key, value) => {
        durable.set(key, value);
      },
      delete: async (key) => {
        durable.delete(key);
      },
      list: async <T>(prefix: string) =>
        new Map(
          [...durable]
            .filter(([key]) => key.startsWith(prefix))
            .map(([key, value]) => [key, value as T]),
        ),
    },
    runtime: {
      get: <T>(key: string) => runtime.get(key) as T | undefined,
      set: (key, value) => {
        runtime.set(key, value);
      },
      delete: (key) => {
        runtime.delete(key);
      },
    },
    ingest: async (event) => {
      ingested.push(event);
      return { status: "queued" };
    },
    fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      const key = `${request.method} ${url.pathname}${url.search}`;
      routes.push({
        method: request.method,
        path: `${url.pathname}${url.search}`,
        body: await request.clone().text(),
      });
      const canned =
        responses.get(key) ?? responses.get(`${request.method} ${url.pathname}`);
      if (!canned) throw new Error(`unexpected Mattermost call: ${key}`);
      return new Response(JSON.stringify(canned.body), {
        status: canned.status,
        headers: { "content-type": "application/json", ...canned.headers },
      });
    }) as typeof fetch,
    scheduleWake: () => undefined,
    settleConversation: async () => undefined,
    setStatus: () => undefined,
    log: () => undefined,
    openWebSocket: async () => {
      throw new Error("not used");
    },
  };

  return {
    ctx,
    routes,
    ingested,
    storage: durable,
    respond(method, path, body, status = 200, headers = {}) {
      responses.set(`${method} ${path}`, { body, status, headers });
    },
  };
}

function seedDiscovery(
  h: Harness,
  channels: Record<string, unknown>[],
  members: Record<string, unknown>[],
) {
  h.respond("GET", "/api/v4/users/me", { id: "bot-1", username: "transit" });
  h.respond("GET", "/api/v4/users/me/channels", channels);
  h.respond(
    "GET",
    "/api/v4/users/me/channel_members?page=0&per_page=200",
    members,
  );
}

describe("Mattermost poller", () => {
  it("costs two calls and reports no activity when nothing is unread", async () => {
    const h = harness();
    seedDiscovery(
      h,
      [{ id: "c1", type: "O", name: "general", total_msg_count: 7 }],
      [{ channel_id: "c1", msg_count: 7, mention_count: 0 }],
    );

    const first = await mattermost.poll!(h.ctx);
    expect(first).toEqual({ activity: false });

    // Identity is cached, so a steady idle tick is exactly the two discovery
    // calls — the property the whole cost model rests on.
    h.routes.length = 0;
    const second = await mattermost.poll!(h.ctx);
    expect(second).toEqual({ activity: false });
    expect(h.routes.map((route) => route.path)).toEqual([
      "/api/v4/users/me/channels",
      "/api/v4/users/me/channel_members?page=0&per_page=200",
    ]);
  });

  it("plants a cursor on first sight instead of replaying history", async () => {
    const h = harness();
    seedDiscovery(
      h,
      [{ id: "c1", type: "D", total_msg_count: 3 }],
      [{ channel_id: "c1", msg_count: 0, mention_count: 0 }],
    );

    const result = await mattermost.poll!(h.ctx);
    expect(result).toEqual({ activity: false });
    expect(h.ingested).toHaveLength(0);
    expect(h.storage.get("cursor:c1")).toEqual(expect.any(Number));
    expect(h.routes.some((route) => route.path.includes("/posts"))).toBe(false);
  });

  it("drains unread direct messages, advances the cursor, and marks read", async () => {
    const h = harness();
    seedDiscovery(
      h,
      [{ id: "c1", type: "D", total_msg_count: 3 }],
      [{ channel_id: "c1", msg_count: 0, mention_count: 0 }],
    );
    await mattermost.poll!(h.ctx);

    // The planted cursor is "now"; a real post always lands after it, and the
    // watermark must never travel backwards.
    const planted = h.storage.get("cursor:c1") as number;
    const postedAt = planted + 1_000;
    h.respond("GET", "/api/v4/channels/c1/posts", {
      order: ["p1"],
      posts: {
        p1: {
          id: "p1",
          channel_id: "c1",
          user_id: "u-9",
          message: "ship it",
          create_at: postedAt,
          update_at: postedAt,
        },
      },
    });
    h.respond("POST", "/api/v4/users/ids", [{ id: "u-9", username: "dana" }]);
    h.respond("POST", "/api/v4/channels/members/bot-1/view", {
      status: "OK",
      last_viewed_at_times: { c1: postedAt },
    });

    h.routes.length = 0;
    const result = await mattermost.poll!(h.ctx);
    expect(result).toEqual({ activity: true });
    expect(h.ingested).toHaveLength(1);
    expect(h.ingested[0]).toMatchObject({
      eventKey: "p1",
      conversationId: "c1",
      trigger: "dm",
      user: "dana",
    });
    expect(h.ingested[0]?.content).toContain("Mattermost dm from dana");
    expect(h.storage.get("cursor:c1")).toBe(postedAt);
    expect(
      h.routes.find((route) => route.path.endsWith("/view"))?.body,
    ).toBe(JSON.stringify({ channel_id: "c1" }));
  });

  it("backs off without throwing when the server answers 429", async () => {
    const h = harness();
    h.respond("GET", "/api/v4/users/me", { id: "bot-1", username: "transit" });
    h.respond("GET", "/api/v4/users/me/channels", { message: "limit exceeded" }, 429, {
      "retry-after": "7",
    });
    h.respond("GET", "/api/v4/users/me/channel_members?page=0&per_page=200", []);

    const result = await mattermost.poll!(h.ctx);
    expect(result).toEqual({ activity: false, backoffMs: 7_000 });
  });
});

describe("Mattermost normalisation", () => {
  const self = { id: "bot-1", username: "transit" };
  const channel = { id: "c1", type: "O", name: "general" };

  it("follows a thread after a mention and keeps following replies", async () => {
    const h = harness();
    const mentioned = await normalizePost(
      h.ctx,
      {
        id: "p1",
        channel_id: "c1",
        user_id: "u-1",
        message: "hey @transit take a look",
        create_at: 1,
      },
      channel,
      self,
      "kim",
    );
    expect(mentioned).toMatchObject({
      eventKey: "p1",
      trigger: "mention",
      conversationId: "c1:p1",
    });

    const reply = await normalizePost(
      h.ctx,
      {
        id: "p2",
        root_id: "p1",
        channel_id: "c1",
        user_id: "u-2",
        message: "agreed",
        create_at: 2,
      },
      channel,
      self,
      "lee",
    );
    expect(reply).toMatchObject({ trigger: "thread", conversationId: "c1:p1" });
  });

  it("ignores near-miss mentions, machine posts, deletions, and its own posts", async () => {
    const h = harness();
    const nearMiss = await normalizePost(
      h.ctx,
      { id: "p1", channel_id: "c1", user_id: "u-1", message: "ask @transitional" },
      channel,
      self,
      "kim",
    );
    expect(nearMiss).toBeNull();

    const machine = await normalizePost(
      h.ctx,
      {
        id: "p2",
        channel_id: "c1",
        user_id: "u-1",
        message: "@transit deploy done",
        props: { from_bot: "true" },
      },
      channel,
      self,
      "ci",
    );
    expect(machine).toBeNull();

    const deleted = await normalizePost(
      h.ctx,
      {
        id: "p3",
        channel_id: "c1",
        user_id: "u-1",
        message: "@transit oops",
        delete_at: 99,
      },
      channel,
      self,
      "kim",
    );
    expect(deleted).toBeNull();

    const own = await normalizePost(
      h.ctx,
      { id: "p4", channel_id: "c1", user_id: "bot-1", message: "@transit echo" },
      channel,
      self,
      "transit",
    );
    expect(own).toBeNull();
  });

  it("keys edits separately and decomposes conversations", async () => {
    const h = harness();
    const edited = await normalizePost(
      h.ctx,
      {
        id: "p1",
        channel_id: "c1",
        user_id: "u-1",
        message: "@transit revised",
        edit_at: 42,
      },
      channel,
      self,
      "kim",
    );
    expect(edited).toMatchObject({ eventKey: "p1:edit:42", trigger: "edit" });
    expect(decomposeConversation("c1:p1")).toEqual({
      channelID: "c1",
      rootID: "p1",
    });
    expect(decomposeConversation("c1")).toEqual({ channelID: "c1", rootID: "" });
  });
});
