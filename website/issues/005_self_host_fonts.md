# Self-Host Fonts

**Status:** Closed
**Priority:** Low
**Tags:** performance, privacy

## Description
Replace the Google Fonts CDN links with self-hosted fonts (using `@fontsource` packages). This improves privacy, performance (no extra DNS lookup), and stability (no layout shifts if CDN is slow).

**Action Items:**
- [ ] Install `@fontsource` packages for `Plus Jakarta Sans`, `Outfit`, and `JetBrains Mono`.
- [ ] Update CSS to import fonts locally.
- [ ] Remove Google Fonts links from `index.html`.
