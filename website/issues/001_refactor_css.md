# Refactor style.css into CSS Modules

**Status:** Open
**Priority:** Medium
**Tags:** code-quality, css, maintenance

## Description
Refactor the monolithic `style.css` (currently 700+ lines) into smaller, modular CSS files (e.g., `components/hero.css`, `layout/footer.css`) imported by `main.js`. This will improve maintainability and readability as the project grows.

**Action Items:**
- [ ] Create a `css` directory in `src`.
- [ ] Split `style.css` into logical modules.
- [ ] Update `main.js` to import the new CSS files.
