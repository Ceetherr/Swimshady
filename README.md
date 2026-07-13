# 🦈 SwimShady

A lightweight, multi-user proxy management panel built on Cloudflare Workers.

## Features

- **Multi-user management** — Create, edit, pause, and delete subscribers with traffic limits and expiry dates
- **Multiple output formats** — URI (base64), Clash YAML, Clash JSON, Sing-box JSON
- **Telegram bot** — Full gateway management via inline buttons
- **Auto-update** — One-click deploy from GitHub directly inside the dashboard
- **Mobile-friendly** — Responsive design with bottom navigation on phones
- **NAT64 support** — Automatic IPv4 to IPv6 address conversion
- **API key system** — Secure node-to-panel authentication
- **Multi-panel sync** — Hub/spoke architecture for managing multiple panels

## Quick Deploy

1. Create a Cloudflare Worker
2. Copy the contents of `_worker.js`
3. Paste into the Worker editor
4. Add a D1 database binding named `IOT_DB`
5. Deploy

## Configuration

After deployment, open the dashboard at:
```
https://your-worker.dev/sync/dash
```

Default password: `admin`

**Change the master key immediately after first login.**

## Supported Protocols

- VLESS (WebSocket)
- Trojan (WebSocket)

## Client Compatibility

| Client | Platform |
|--------|----------|
| Clash Verge / Mihomo | Desktop |
| Sing-box | Desktop / Mobile |
| V2rayNG | Android |
| Streisand | iOS |
| Nekobox | Desktop |

## Dashboard

The admin dashboard includes:

- **Overview** — User stats, traffic, recent activity
- **Endpoints** — Subscription profiles and sync links
- **Network** — Origin IP, edge node, region, connection metrics
- **Settings** — Protocol, ports, UUID, API route, master key
- **Advanced** — DNS, proxy IPs, NAT64, name strategy, Telegram, Cloudflare
- **Logs** — Activity log feed
- **Users** — Subscriber management with traffic monitoring

## Telegram Bot

Manage your panel from Telegram:

- Add/edit/delete subscribers
- View traffic stats
- Toggle pause/resume
- Panic mode (randomize routes + pause all)
- Multi-panel switching

## Environment Variables

| Binding | Type | Description |
|---------|------|-------------|
| `IOT_DB` | D1 Database | Stores config and usage data |

## License

MIT
