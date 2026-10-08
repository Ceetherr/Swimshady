# Changelog

## v1.3.0

### Added
- TLS fragmentation (segmentation) — split ClientHello into multiple packets to evade SNI-based filtering
- TLS cipher mask — control which ciphers the ClientHello advertises
- Relay self-healing — dead relays are quarantined, probed, and automatically buried/resurrected
- Telegram: copy sub link button on user detail
- Telegram: full user creation with ports, mode, proxy, clean IPs, and device limit
- Telegram: relay status menu showing healthy and quarantined relays
- Telegram: bulk operations — reset all traffic, extend all expiry, bulk delete with selection
- Telegram: notification preferences — toggle 10 alert types individually
- Telegram: per-user config links for Clash, sing-box, v2rayN, and Raw

### Fixed
- maxConfigs producing wrong line count — now generates exactly the requested number of configs
- Proxies not all appearing in configs — per-user and global proxy lists are now combined
- Reset not reflected in sub link or app — usage epoch invalidation fixes stale data across isolates
- Live profile usage showing zero — uuidUsage now merges across isolates via D1
- Update notification gaps — idle re-check, dismiss persistence, pre-release version handling

### Improved
- User rows now use a fixed grid layout — usage bars are perfectly aligned across all rows
- Ports shown as a count badge with full list in hover tooltip
- Expiry shows remaining days with color coding (yellow under 7 days, red when expired)
- Usage text no longer truncated — used/total on left, remaining on right

## v1.2.1

### Fixed
- Custom Config Name not applied — user `customName` field was never saved on create/update and was not used in config name generation
- Custom Config Name not shown in subscription links — profile builders now pass per-user `customName` to `getConfigName`
- Subscription info page now displays custom name instead of internal user name
- Update notification banner not showing — inline `style="display:none"` was overriding Tailwind `hidden` class, preventing the banner from ever becoming visible

## v1.2.0

### Added
- Private DoH Server endpoint — use your worker as a DNS-over-HTTPS server for privacy and censorship bypass
- DNS-based ad blocking — blocks 60+ ad/tracker domains including Google, Facebook, TikTok, and analytics trackers
- Subscription page format buttons — copy links for Clash, Sing-box, and Raw formats with app hints
- Live connection data persistence — active connection counts now survive worker restarts via D1
- Panel name displayed in subscription page credits

### Fixed
- Live profile usage always showing "No active connection data yet" — data now persists to D1

### Improved
- Subscription page now shows which app works with which format (Clash Verge, Sing-box, V2rayNG, etc.)
- DoH server supports GET (JSON API) and POST (RFC 8484 wire format) with CORS headers

## v1.1.0

### Added
- Update notification banner restored — panel now detects new GitHub versions and offers one-click deploy
- Mobile bottom navigation with responsive grids — stats adapt to phone screens
- XSS protection on subscription page — user names are now HTML-escaped
- Shark emoji branding

### Fixed
- Fake configs toggle not working — was missing CSS class and onclick handler
- Add/Edit User NAT64 field swap — each form now reads from its own field
- Duplicate element ID breaking network view active connections count
- Dashboard crash from missing `ov-today-reqs` element reference
- "swin shady" typo fixed to "swim shady" everywhere

### Improved
- Color contrast for better readability — muted text now meets WCAG standards
- Removed unused Persian translations from web dashboard
