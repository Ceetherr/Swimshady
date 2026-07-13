# Changelog

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
