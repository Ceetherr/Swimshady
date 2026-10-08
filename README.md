# 🦈 SwimShady

A lightweight, multi-user proxy management panel built on Cloudflare Workers.

## Features

- **Multi-user management** — Create, edit, pause, and delete subscribers with traffic limits and expiry dates
- **Multiple output formats** — URI (base64), Clash YAML, Clash JSON, Sing-box JSON, v2rayN JSON
- **TLS fragmentation** — Split ClientHello into multiple packets to evade SNI-based filtering
- **TLS cipher mask** — Control which ciphers the ClientHello advertises
- **Relay self-healing** — Dead relays are quarantined, probed, and automatically buried/resurrected
- **Telegram bot** — Full gateway management: users, bulk ops, relay status, notifications, config links
- **Auto-update** — One-click deploy from GitHub directly inside the dashboard
- **Mobile-friendly** — Responsive design with bottom navigation on phones
- **NAT64 support** — Automatic IPv4 to IPv6 address conversion
- **API key system** — Secure node-to-panel authentication
- **Multi-panel sync** — Hub/spoke architecture for managing multiple panels
- **DoH proxy** — Built-in DNS-over-HTTPS server
- **Live usage** — Real-time connection and traffic data across isolates

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
|---|---|
| Sing-box | Desktop / Mobile |
| V2rayNG | Android |
| Streisand | iOS |
| v2box | iOS |
| Nekobox | Desktop |
| V2rayN | Desktop |

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

- Add/edit/delete subscribers with full options (ports, mode, proxy, clean IPs, device limit)
- View traffic stats and per-user details
- Toggle pause/resume
- Reset traffic, extend expiry
- Bulk operations — reset all, extend all, bulk delete with selection
- Relay status — view healthy and quarantined relays
- Notification preferences — toggle 10 alert types individually
- Per-user config links — Clash, sing-box, v2rayN, Raw
- Panic mode (randomize routes + pause all)
- Multi-panel switching

## Environment Variables

| Binding | Type | Description |
|---|---|---|
| `IOT_DB` | D1 Database | Stores config and usage data |

## License

MIT
