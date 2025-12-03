# Issue Tracking Workflow

This directory contains the issue tickets for VarveDB. We use a simple file-based issue tracking system.

## Creating an Issue

1.  **File Naming:** Create a new markdown file in this directory with the format `XXX-descriptive-slug.md`, where `XXX` is a sequential 3-digit number (e.g., `013-optimize-wal.md`).
2.  **Frontmatter:** Include the following YAML frontmatter at the top of the file:

    ```yaml
    ---
    title: "[Category] Title of the Issue"
    tags: ["tag1", "tag2"]
    priority: "High" | "Medium" | "Low"
    status: "Open"
    ---
    ```

3.  **Content:** Describe the problem, proposed solution, and acceptance criteria.

## Managing Issues

*   **Closing an Issue:** When an issue is completed, change the `status` field in the frontmatter from `"Open"` to `"Closed"`. Do **not** delete the file.
*   **Finding Open Issues:** You can easily list all open issues using `grep`:

    ```bash
    grep -l '^status: "Open"' issues/*.md
    ```

## Issue Lifecycle

1.  **Open:** The issue is created and ready to be picked up.
2.  **In Progress:** (Optional) You can update the status to "In Progress" if you are actively working on it.
3.  **Closed:** The work is done and verified.
