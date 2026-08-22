import { describe, expect, it } from "vitest";
import type { ConnectorCtx } from "transit-connector-kit";
import { decomposeConversation, normalizeMattermostEvent } from "../src";

function context(): ConnectorCtx {
  const durable = new Map<string, unknown>();
  const runtime = new Map<string, unknown>();
  return {
    config: { bot_user_id: "bot-1" },
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
    ingest: async () => ({ status: "queued" }),
    fetch,
    scheduleWake: () => undefined,
    settleConversation: async () => undefined,
    setStatus: () => undefined,
    log: () => undefined,
    openWebSocket: async () => {
      throw new Error("not used");
    },
  };
}

function event(post: Record<string, unknown>, mentions: string[] = []) {
  return {
    event: "posted",
    data: {
      post: JSON.stringify(post),
      channel_type: "O",
      channel_name: "ops",
      sender_name: "@ada",
      mentions: JSON.stringify(mentions),
    },
  };
}

describe("Mattermost connector", () => {
  it("starts a followed thread on mention and keeps following replies", async () => {
    const ctx = context();
    const mentioned = await normalizeMattermostEvent(
      ctx,
      event(
        {
          id: "root-1",
          channel_id: "channel-1",
          user_id: "user-1",
          message: "@bot investigate",
        },
        ["bot-1"],
      ),
    );
    expect(mentioned).toMatchObject({
      eventKey: "root-1",
      conversationId: "channel-1:root-1",
      trigger: "mention",
      user: "ada",
    });

    const reply = await normalizeMattermostEvent(
      ctx,
      event({
        id: "reply-1",
        root_id: "root-1",
        channel_id: "channel-1",
        user_id: "user-2",
        message: "following up",
      }),
    );
    expect(reply).toMatchObject({
      conversationId: "channel-1:root-1",
      trigger: "thread",
    });
  });

  it("drops bot posts and decomposes conversations", async () => {
    expect(
      await normalizeMattermostEvent(
        context(),
        event({
          id: "bot-post",
          channel_id: "channel-1",
          user_id: "bot-1",
          message: "loop",
        }),
      ),
    ).toBeNull();
    expect(decomposeConversation("channel-1:root-1")).toEqual({
      channelID: "channel-1",
      rootID: "root-1",
    });
    expect(decomposeConversation("dm-channel")).toEqual({
      channelID: "dm-channel",
      rootID: "",
    });
  });
});
