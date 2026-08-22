# Transit Connector: Mattermost

Build-time Mattermost adapter for Transit. Maintains an authenticated outbound WebSocket, ingests direct messages and mentions, follows mentioned threads in Durable Object storage, and posts replies through the Mattermost API.

## Configuration

- `server_url` — Mattermost origin.
- `bot_token` — personal access token for the bot; secret.
- `bot_user_id` — bot user ID used for mention and loop filtering.
- `reply_mode` — default `root` or `thread` behavior for direct messages.
- `instructions` — optional text appended to `read_message` results.

Create a Mattermost bot account, grant it access to the relevant teams/channels, and generate a token. Transit opens `<server_url>/api/v4/websocket` with `Authorization: Bearer` and posts replies to `/api/v4/posts`.

A channel mention starts a followed thread. Later posts in that thread continue arriving with `trigger="thread"` even without another mention. Attachments are reported in event content; file download is outside v1.

```bash
bun install
bun run typecheck
bun run test
```

MIT licensed.
