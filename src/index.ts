import {
  CONNECTOR_API,
  type Connector,
  type ConnectorCtx,
  type ConnectorEvent,
  type ReplyRequest,
} from "transit-connector-kit";

type MattermostPost = {
  id: string;
  root_id?: string;
  channel_id: string;
  user_id: string;
  message?: string;
  edit_at?: number;
  file_ids?: string[];
  metadata?: { files?: unknown[] };
  props?: Record<string, unknown>;
};

type MattermostEvent = {
  event?: string;
  data?: {
    post?: string;
    channel_type?: string;
    channel_name?: string;
    channel_display_name?: string;
    sender_name?: string;
    mentions?: string;
  };
};

function machinePost(post: MattermostPost): boolean {
  const truthy = (value: unknown) => value === true || value === "true";
  return truthy(post.props?.from_bot) || truthy(post.props?.from_webhook);
}

async function normalizeMattermostEvent(
  ctx: ConnectorCtx,
  frame: MattermostEvent,
): Promise<ConnectorEvent | null> {
  if (
    (frame.event !== "posted" && frame.event !== "post_edited") ||
    !frame.data?.post
  ) {
    return null;
  }
  let post: MattermostPost;
  try {
    post = JSON.parse(frame.data.post) as MattermostPost;
  } catch {
    return null;
  }
  if (!post.id || !post.channel_id || post.user_id === ctx.config.bot_user_id || machinePost(post)) {
    return null;
  }

  let mentions: string[] = [];
  try {
    mentions = JSON.parse(frame.data.mentions || "[]") as string[];
  } catch {
    mentions = [];
  }
  const isDM = frame.data.channel_type === "D";
  const mentioned = Boolean(
    ctx.config.bot_user_id && mentions.includes(ctx.config.bot_user_id),
  );
  const rootID = post.root_id || post.id;
  const threadKey = `thread:${post.channel_id}:${rootID}`;
  let trigger: string;
  if (isDM) {
    trigger = frame.event === "post_edited" ? "edit" : "dm";
  } else if (mentioned) {
    trigger = frame.event === "post_edited" ? "edit" : "mention";
    await ctx.storage.put(threadKey, { followedAt: Date.now() });
  } else if (post.root_id && (await ctx.storage.get(threadKey))) {
    trigger = frame.event === "post_edited" ? "edit" : "thread";
  } else {
    return null;
  }

  const sender = frame.data.sender_name?.replace(/^@/u, "") || post.user_id;
  let body = post.message?.trim() || "";
  const attachmentCount = Math.max(
    post.file_ids?.length ?? 0,
    post.metadata?.files?.length ?? 0,
  );
  if (attachmentCount > 0) {
    body += `${body ? "\n\n" : ""}[Mattermost attachments: ${attachmentCount}]`;
  }
  if (!body) body = "(empty Mattermost message)";
  const channelName = frame.data.channel_name || frame.data.channel_display_name;
  const location = isDM ? "a direct message" : channelName || "a channel";
  const content = `Mattermost ${trigger} from ${sender} in ${location}:\n\n${body}`;
  return {
    eventKey:
      frame.event === "post_edited"
        ? `${post.id}:edit:${post.edit_at ?? 0}`
        : post.id,
    conversationId:
      isDM && !post.root_id
        ? post.channel_id
        : `${post.channel_id}:${rootID}`,
    user: sender,
    trigger,
    content,
    meta: {
      channel_id: post.channel_id,
      post_id: post.id,
      root_id: rootID,
      channel_type: frame.data.channel_type || "",
      ...(channelName ? { channel_name: channelName } : {}),
      ...(attachmentCount > 0 ? { attachments: String(attachmentCount) } : {}),
    },
  };
}

function socketURL(serverURL: string): string {
  const url = new URL(serverURL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `${url.pathname.replace(/\/$/u, "")}/api/v4/websocket`;
  return url.toString();
}

async function start(ctx: ConnectorCtx): Promise<void> {
  const existing = ctx.runtime.get<WebSocket>("socket");
  if (existing?.readyState === WebSocket.OPEN) return;
  const serverURL = ctx.config.server_url;
  const botToken = ctx.config.bot_token;
  if (!serverURL || !botToken) throw new Error("Mattermost connector is not configured");
  const socket = await ctx.openWebSocket(
    socketURL(serverURL),
    undefined,
    { Authorization: `Bearer ${botToken}` },
  );
  ctx.runtime.set("socket", socket);
  socket.addEventListener("open", () => ctx.setStatus({ state: "connected" }));
  socket.addEventListener("message", (message) => {
    void (async () => {
      let frame: MattermostEvent;
      try {
        frame = JSON.parse(String(message.data)) as MattermostEvent;
      } catch {
        return;
      }
      const event = await normalizeMattermostEvent(ctx, frame);
      if (event) await ctx.ingest(event);
    })().catch((error) => {
      ctx.log("error", "Mattermost event failed", {
        error: error instanceof Error ? error.message : String(error),
      });
    });
  });
  socket.addEventListener("close", () => {
    ctx.runtime.delete("socket");
    ctx.setStatus({ state: "error", detail: "socket disconnected" });
    ctx.scheduleWake(5_000);
  });
  socket.addEventListener("error", () => {
    ctx.setStatus({ state: "error", detail: "socket error" });
  });
  ctx.setStatus({ state: "connected" });
  ctx.scheduleWake(30_000);
}

async function wake(ctx: ConnectorCtx): Promise<void> {
  const socket = ctx.runtime.get<WebSocket>("socket");
  if (!socket || socket.readyState !== WebSocket.OPEN) await start(ctx);
  ctx.scheduleWake(30_000);
}

async function stop(ctx: ConnectorCtx): Promise<void> {
  const socket = ctx.runtime.get<WebSocket>("socket");
  if (socket && socket.readyState < WebSocket.CLOSING) {
    socket.close(1000, "integration stopped");
  }
  ctx.runtime.delete("socket");
}

function decomposeConversation(conversationID: string): {
  channelID: string;
  rootID: string;
} {
  const separator = conversationID.indexOf(":");
  if (separator < 0) return { channelID: conversationID, rootID: "" };
  return {
    channelID: conversationID.slice(0, separator),
    rootID: conversationID.slice(separator + 1),
  };
}

async function postReply(ctx: ConnectorCtx, request: ReplyRequest): Promise<void> {
  const serverURL = ctx.config.server_url;
  const botToken = ctx.config.bot_token;
  if (!serverURL || !botToken) throw new Error("Mattermost connector is not configured");
  const { channelID, rootID } = decomposeConversation(request.conversationId);
  const isDM = request.event.meta?.channel_type === "D";
  const replyMode = request.replyMode ?? ctx.config.reply_mode;
  let replyRoot = rootID;
  if (isDM && replyMode === "root") replyRoot = "";
  if (isDM && replyMode === "thread" && !replyRoot) {
    replyRoot = request.event.meta?.root_id || request.event.meta?.post_id || "";
  }
  const response = await ctx.fetch(
    `${serverURL.replace(/\/$/u, "")}/api/v4/posts`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${botToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        channel_id: channelID,
        message: request.message,
        ...(replyRoot ? { root_id: replyRoot } : {}),
      }),
    },
  );
  if (!response.ok) {
    throw new Error(`Mattermost post failed (${response.status}): ${await response.text()}`);
  }
  await ctx.settleConversation(request.conversationId);
}

const mattermost = {
  api: CONNECTOR_API,
  name: "mattermost",
  mode: "socket",
  configFields: [
    { key: "server_url", label: "Server URL", required: true },
    { key: "bot_token", label: "Bot token", secret: true, required: true },
    { key: "bot_user_id", label: "Bot user ID" },
    { key: "reply_mode", label: "Default reply mode", placeholder: "thread" },
    { key: "instructions", label: "Agent instructions" },
  ],
  start,
  wake,
  stop,
  postReply,
} satisfies Connector;

export { decomposeConversation, normalizeMattermostEvent };
export default mattermost;
