import {
  CONNECTOR_API,
  type Connector,
  type ConnectorAttachment,
  type ConnectorCtx,
  type ConnectorEvent,
  type PollResult,
  type ReplyRequest,
} from "transit-connector-kit";

/**
 * Mattermost connector.
 *
 * Mattermost already owns the durable state — posts, channels, threads, read
 * marks — so this connector holds no socket and stores only what Mattermost
 * cannot answer for us: which threads the bot has been pulled into, and how far
 * each channel has been consumed.
 *
 * Every cycle costs two cross-team discovery calls; per-channel post deltas are
 * fetched only for channels Mattermost itself reports as unread. The host owns
 * cadence (see `Connector.poll`), so this function never sleeps or reschedules.
 */

type MattermostPost = {
  id: string;
  root_id?: string;
  channel_id: string;
  user_id: string;
  message?: string;
  create_at?: number;
  update_at?: number;
  edit_at?: number;
  delete_at?: number;
  file_ids?: string[];
  metadata?: { files?: unknown[] };
  props?: Record<string, unknown>;
};

type MattermostFileInfo = {
  id: string;
  name?: string;
  mime_type?: string;
  size?: number;
};

type MattermostChannel = {
  id: string;
  type: string;
  name?: string;
  display_name?: string;
  total_msg_count?: number;
  delete_at?: number;
};

type MattermostChannelMember = {
  channel_id: string;
  msg_count?: number;
  mention_count?: number;
};

type MattermostUser = { id: string; username: string };

type PostsResponse = {
  order?: string[];
  posts?: Record<string, MattermostPost>;
};

type Identity = { id: string; username: string };
const FIRST_SIGHT_LOOKBACK_MS = 2 * 60_000;

// One cycle never fans out without bound: a backlog is drained across cycles,
// and reporting activity keeps the host at burst cadence until it is gone.
const MAX_CHANNELS_PER_CYCLE = 12;
const MAX_POSTS_PER_CHANNEL = 60;
const MEMBER_PAGE_SIZE = 200;
const MAX_MEMBER_PAGES = 5;
const DEFAULT_BACKOFF_MS = 5_000;

class RateLimited extends Error {
  constructor(readonly retryAfterMs: number) {
    super(`Mattermost rate limited; retry in ${retryAfterMs}ms`);
  }
}

function base(ctx: ConnectorCtx): string {
  const serverURL = ctx.config.server_url;
  if (!serverURL || !ctx.config.bot_token) {
    throw new Error("Mattermost connector is not configured");
  }
  return serverURL.replace(/\/$/u, "");
}

async function call<T>(
  ctx: ConnectorCtx,
  path: string,
  init?: { method?: string; body?: unknown },
): Promise<T> {
  const response = await ctx.fetch(`${base(ctx)}${path}`, {
    method: init?.method ?? "GET",
    headers: {
      authorization: `Bearer ${ctx.config.bot_token}`,
      ...(init?.body === undefined ? {} : { "content-type": "application/json" }),
    },
    ...(init?.body === undefined ? {} : { body: JSON.stringify(init.body) }),
  });
  if (response.status === 429) {
    const retryAfter = Number(response.headers.get("retry-after"));
    throw new RateLimited(
      Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter * 1_000
        : DEFAULT_BACKOFF_MS,
    );
  }
  if (!response.ok) {
    throw new Error(
      `Mattermost ${path} failed (${response.status}): ${await response.text()}`,
    );
  }
  return (await response.json()) as T;
}

function fileInfo(value: unknown): MattermostFileInfo | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.id !== "string" || !candidate.id) return null;
  return {
    id: candidate.id,
    ...(typeof candidate.name === "string" && candidate.name
      ? { name: candidate.name }
      : {}),
    ...(typeof candidate.mime_type === "string" && candidate.mime_type
      ? { mime_type: candidate.mime_type }
      : {}),
    ...(typeof candidate.size === "number" && Number.isFinite(candidate.size)
      ? { size: candidate.size }
      : {}),
  };
}

async function postAttachments(
  ctx: ConnectorCtx,
  post: MattermostPost,
): Promise<ConnectorAttachment[]> {
  const byID = new Map<string, MattermostFileInfo>();
  for (const value of post.metadata?.files ?? []) {
    const info = fileInfo(value);
    if (info) byID.set(info.id, info);
  }
  for (const id of post.file_ids ?? []) {
    if (byID.has(id)) continue;
    try {
      byID.set(id, await call<MattermostFileInfo>(
        ctx,
        `/api/v4/files/${encodeURIComponent(id)}/info`,
      ));
    } catch (error) {
      ctx.log("warn", "Mattermost file metadata unavailable", {
        fileId: id,
        error: error instanceof Error ? error.message : String(error),
      });
      byID.set(id, { id });
    }
  }
  return [...byID.values()].slice(0, 16).map((info) => ({
    id: info.id,
    name: info.name || `mattermost-${info.id}`,
    ...(info.mime_type ? { contentType: info.mime_type } : {}),
    ...(info.size !== undefined ? { size: info.size } : {}),
  }));
}

function machinePost(post: MattermostPost): boolean {
  const truthy = (value: unknown) => value === true || value === "true";
  return truthy(post.props?.from_bot) || truthy(post.props?.from_webhook);
}

/**
 * The bot's own identity. `bot_user_id` is honoured when configured, but the
 * username is always resolved from the server because mention matching needs
 * the exact handle.
 */
async function identity(ctx: ConnectorCtx): Promise<Identity> {
  const cached = ctx.runtime.get<Identity>("identity");
  if (cached) return cached;
  const stored = await ctx.storage.get<Identity>("identity");
  if (stored) {
    ctx.runtime.set("identity", stored);
    return stored;
  }
  const me = await call<MattermostUser>(ctx, "/api/v4/users/me");
  const resolved: Identity = {
    id: ctx.config.bot_user_id || me.id,
    username: me.username,
  };
  await ctx.storage.put("identity", resolved);
  ctx.runtime.set("identity", resolved);
  return resolved;
}

async function channelMembers(
  ctx: ConnectorCtx,
): Promise<Map<string, MattermostChannelMember>> {
  const members = new Map<string, MattermostChannelMember>();
  for (let page = 0; page < MAX_MEMBER_PAGES; page += 1) {
    const batch = await call<MattermostChannelMember[]>(
      ctx,
      `/api/v4/users/me/channel_members?page=${page}&per_page=${MEMBER_PAGE_SIZE}`,
    );
    for (const member of batch) members.set(member.channel_id, member);
    if (batch.length < MEMBER_PAGE_SIZE) break;
  }
  return members;
}

/** Resolve post authors to usernames, one batched call per cycle. */
async function usernames(
  ctx: ConnectorCtx,
  ids: Set<string>,
): Promise<Map<string, string>> {
  const resolved = new Map<string, string>();
  const missing: string[] = [];
  for (const id of ids) {
    const cached = await ctx.storage.get<string>(`user:${id}`);
    if (cached) resolved.set(id, cached);
    else missing.push(id);
  }
  if (missing.length > 0) {
    const users = await call<MattermostUser[]>(ctx, "/api/v4/users/ids", {
      method: "POST",
      body: missing,
    });
    for (const user of users) {
      resolved.set(user.id, user.username);
      await ctx.storage.put(`user:${user.id}`, user.username);
    }
  }
  return resolved;
}

/**
 * Turn one fetched post into a Transit event, or `null` when the bot has no
 * business reacting to it. Trigger precedence matches the rest of Transit:
 * a direct message, an explicit mention, or a reply inside a thread the bot
 * was already pulled into.
 */
async function normalizePost(
  ctx: ConnectorCtx,
  post: MattermostPost,
  channel: MattermostChannel,
  self: Identity,
  author: string,
): Promise<ConnectorEvent | null> {
  if (!post.id || !post.channel_id) return null;
  if (post.delete_at && post.delete_at > 0) return null;
  if (post.user_id === self.id || machinePost(post)) return null;

  const isDM = channel.type === "D";
  const message = post.message ?? "";
  const mentioned =
    Boolean(self.username) &&
    new RegExp(`(^|\\W)@${self.username}(\\b|$)`, "u").test(message);
  const rootID = post.root_id || post.id;
  const threadKey = `thread:${post.channel_id}:${rootID}`;
  const edited = Boolean(post.edit_at && post.edit_at > 0);

  let trigger: string;
  if (isDM) {
    trigger = edited ? "edit" : "dm";
  } else if (mentioned) {
    trigger = edited ? "edit" : "mention";
    await ctx.storage.put(threadKey, { followedAt: Date.now() });
  } else if (post.root_id && (await ctx.storage.get(threadKey))) {
    trigger = edited ? "edit" : "thread";
  } else {
    return null;
  }

  let body = message.trim();
  const attachments = await postAttachments(ctx, post);
  const attachmentCount = attachments.length;
  if (attachmentCount > 0) {
    body += `${body ? "\n\n" : ""}[Mattermost attachments: ${attachmentCount}]`;
  }
  if (!body) body = "(empty Mattermost message)";
  const channelName = channel.name || channel.display_name;
  const location = isDM ? "a direct message" : channelName || "a channel";
  const content = `Mattermost ${trigger} from ${author} in ${location}:\n\n${body}`;
  return {
    eventKey: edited ? `${post.id}:edit:${post.edit_at ?? 0}` : post.id,
    conversationId:
      isDM && !post.root_id ? post.channel_id : `${post.channel_id}:${rootID}`,
    user: author,
    trigger,
    content,
    ...(attachments.length > 0 ? { attachments } : {}),
    meta: {
      channel_id: post.channel_id,
      post_id: post.id,
      root_id: rootID,
      channel_type: channel.type,
      ...(channelName ? { channel_name: channelName } : {}),
      ...(attachmentCount > 0 ? { attachments: String(attachmentCount) } : {}),
    },
  };
}

/** Drain one channel's unread posts. Returns whether anything was ingested. */
async function drainChannel(
  ctx: ConnectorCtx,
  channel: MattermostChannel,
  self: Identity,
): Promise<boolean> {
  const cursorKey = `cursor:${channel.id}`;
  // A channel can be created by its first DM. Starting at "now" silently drops
  // that exact message, then marks the new channel read. A bounded lookback
  // admits the first post without turning connector startup into a history
  // replay; older posts are rejected again below by create_at < cursor.
  const cursor =
    (await ctx.storage.get<number>(cursorKey)) ??
    Date.now() - FIRST_SIGHT_LOOKBACK_MS;

  const page = await call<PostsResponse>(
    ctx,
    `/api/v4/channels/${channel.id}/posts?since=${cursor}`,
  );
  const posts = page.posts ?? {};
  // `order` is newest-first and absent for `since` responses on some versions;
  // sort explicitly so events are ingested oldest-first.
  const ordered = Object.values(posts)
    .sort((a, b) => (a.create_at ?? 0) - (b.create_at ?? 0))
    .slice(0, MAX_POSTS_PER_CHANNEL);

  const authorIds = new Set(
    ordered.filter((post) => post.user_id !== self.id).map((post) => post.user_id),
  );
  const authors = authorIds.size > 0 ? await usernames(ctx, authorIds) : new Map();

  let ingested = false;
  let watermark = cursor;
  for (const post of ordered) {
    watermark = Math.max(watermark, post.update_at ?? post.create_at ?? watermark);
    // `since` selects by UPDATE time, so replying in an old thread bumps its
    // root and hands it back as if it were new. A post created before the
    // cursor is not news whatever moved it: the cursor is only ever set to a
    // time we had already caught up to, so anything older than it was either
    // already relayed or predates this channel being watched. Without this an
    // agent's own reply resurfaces the human post it was replying to - four
    // days late, indistinguishable from a fresh request.
    if ((post.create_at ?? 0) < cursor) continue;
    const event = await normalizePost(
      ctx,
      post,
      channel,
      self,
      authors.get(post.user_id) ?? post.user_id,
    );
    if (!event) continue;
    // A duplicate is not news: re-seeing a post must not pin the host at
    // burst cadence.
    const { status } = await ctx.ingest(event);
    if (status === "queued") ingested = true;
  }
  await ctx.storage.put(cursorKey, watermark);

  // Clearing the unread mark is what keeps the two-call probe meaningful; do it
  // only after the posts have actually been consumed.
  await call(ctx, `/api/v4/channels/members/${self.id}/view`, {
    method: "POST",
    body: { channel_id: channel.id },
  });
  return ingested;
}

async function poll(ctx: ConnectorCtx): Promise<PollResult> {
  try {
    const self = await identity(ctx);
    const [channels, members] = await Promise.all([
      call<MattermostChannel[]>(ctx, "/api/v4/users/me/channels"),
      channelMembers(ctx),
    ]);

    const unread = channels
      .filter((channel) => !channel.delete_at)
      .filter((channel) => {
        const member = members.get(channel.id);
        if (!member) return false;
        const behind = (channel.total_msg_count ?? 0) - (member.msg_count ?? 0);
        return behind > 0 || (member.mention_count ?? 0) > 0;
      });

    if (unread.length === 0) {
      ctx.setStatus({ state: "polling" });
      return { activity: false };
    }

    let activity = false;
    for (const channel of unread.slice(0, MAX_CHANNELS_PER_CYCLE)) {
      if (await drainChannel(ctx, channel, self)) activity = true;
    }
    ctx.setStatus({ state: "polling" });
    // A backlog wider than one cycle keeps the host at burst cadence.
    return { activity: activity || unread.length > MAX_CHANNELS_PER_CYCLE };
  } catch (error) {
    if (error instanceof RateLimited) {
      ctx.setStatus({ state: "error", detail: "rate limited" });
      return { activity: false, backoffMs: error.retryAfterMs };
    }
    throw error;
  }
}

async function start(ctx: ConnectorCtx): Promise<void> {
  await identity(ctx);
  ctx.setStatus({ state: "polling" });
}

async function stop(ctx: ConnectorCtx): Promise<void> {
  ctx.runtime.delete("identity");
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
  const { channelID, rootID } = decomposeConversation(request.conversationId);
  const isDM = request.event.meta?.channel_type === "D";
  const replyMode = request.replyMode ?? ctx.config.reply_mode;
  let replyRoot = rootID;
  if (isDM && replyMode === "root") replyRoot = "";
  if (isDM && replyMode === "thread" && !replyRoot) {
    replyRoot = request.event.meta?.root_id || request.event.meta?.post_id || "";
  }
  await call(ctx, "/api/v4/posts", {
    method: "POST",
    body: {
      channel_id: channelID,
      message: request.message,
      ...(replyRoot ? { root_id: replyRoot } : {}),
    },
  });
  await ctx.settleConversation(request.conversationId);
}

async function fetchAttachment(
  ctx: ConnectorCtx,
  _event: ConnectorEvent,
  attachment: ConnectorAttachment,
): Promise<Response> {
  const response = await ctx.fetch(
    `${base(ctx)}/api/v4/files/${encodeURIComponent(attachment.id)}`,
    { headers: { authorization: `Bearer ${ctx.config.bot_token}` } },
  );
  if (!response.ok) {
    throw new Error(
      `Mattermost attachment ${attachment.id} failed (${response.status})`,
    );
  }
  return response;
}

const mattermost = {
  api: CONNECTOR_API,
  name: "mattermost",
  mode: "poll",
  configFields: [
    { key: "server_url", label: "Server URL", required: true },
    { key: "bot_token", label: "Bot token", secret: true, required: true },
    {
      key: "bot_user_id",
      label: "Bot user ID",
      help: "Optional — resolved from the token when blank.",
    },
    { key: "reply_mode", label: "Default reply mode", placeholder: "thread" },
    { key: "instructions", label: "Agent instructions" },
  ],
  start,
  poll,
  stop,
  fetchAttachment,
  postReply,
} satisfies Connector;

export { decomposeConversation, normalizePost };
export default mattermost;
