import { defineConfig } from 'astro/config';
import remarkGithubBlockquoteAlert from 'remark-github-blockquote-alert';
import remarkGfm from 'remark-gfm';

export default defineConfig({
  site: 'https://varvedb.com',
  compressHTML: true,
  markdown: {
    remarkPlugins: [
      remarkGithubBlockquoteAlert,
      remarkGfm,
    ],
  },
});
