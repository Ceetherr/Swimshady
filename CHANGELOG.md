# Changelog

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
