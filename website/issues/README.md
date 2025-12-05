# Website Issues

This directory contains issue tickets for the VarveDB website. We use a flat-file issue tracking system to keep everything close to the code.

## How to use

### Listing Issues
You can use standard command-line tools to list and filter issues.

**List all open issues:**
```bash
grep -l "**Status:** Open" *.md
```

**List high priority issues:**
```bash
grep -l "**Priority:** High" *.md
```

**Find issues by tag (e.g., 'css'):**
```bash
grep -l "**Tags:**.*css" *.md
```

### Creating a New Issue
Create a new markdown file in this directory with the following format:

```markdown
# Issue Title

**Status:** Open
**Priority:** Low|Medium|High
**Tags:** tag1, tag2

## Description
Describe the issue or improvement here.
```

### Closing an Issue
Edit the file and change the status line to:
`**Status:** Closed`
