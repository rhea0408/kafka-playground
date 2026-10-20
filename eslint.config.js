// @ts-check
import js from '@eslint/js';
import { defineConfig } from 'eslint/config';
import prettier from 'eslint-config-prettier';
import globals from 'globals';
import tseslint from 'typescript-eslint';

/** Packages that run in a Web Worker, in Node tests and in the fidelity harness (CLAUDE.md rule 1). */
const HEADLESS = [
  'packages/engine/**/*.ts',
  'packages/level-runtime/**/*.ts',
  'packages/schema/**/*.ts',
];

/** Packages whose results must be deterministic (CLAUDE.md rule 2). */
const DETERMINISTIC = ['packages/engine/**/*.ts', 'packages/level-runtime/**/*.ts'];

/**
 * Every browser global that is not also a plain ECMAScript built-in: window, document, fetch,
 * console, setTimeout, performance, …. Headless packages must not touch any of them.
 */
const browserOnlyGlobals = Object.keys(globals.browser).filter((name) => !(name in globals.es2023));

const nondeterministic = 'Non-deterministic. Use the seeded RNG / sim clock from core/.';

const namedExportsOnly = { selector: 'ExportDefaultDeclaration', message: 'Use named exports.' };

/** Web app files: React in the browser main thread (M2-T0). */
const WEB = ['apps/web/src/**/*.{ts,tsx}'];

/** Web Worker entry points (the sim host, M2-T4). They get worker globals instead of DOM ones. */
const WEB_WORKERS = ['apps/web/src/**/*.worker.ts'];

/** Tool config and Playwright specs for the web app; they run in Node. */
const WEB_NODE = ['apps/web/*.config.ts', 'apps/web/e2e/**/*.ts'];

export default defineConfig(
  {
    ignores: [
      '**/node_modules/**',
      '**/dist/**',
      '**/coverage/**',
      '**/playwright-report/**',
      '**/test-results/**',
    ],
  },

  {
    extends: [js.configs.recommended, tseslint.configs.strictTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: { allowDefaultProject: ['*.js'] },
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/switch-exhaustiveness-check': 'error',
      '@typescript-eslint/consistent-type-imports': 'error',
      // `const { omitted, ...rest } = obj` is the idiomatic way to drop a field.
      '@typescript-eslint/no-unused-vars': ['error', { ignoreRestSiblings: true }],
      // Named exports only (CLAUDE.md code style).
      'no-restricted-syntax': ['error', namedExportsOnly],
    },
  },

  {
    files: HEADLESS,
    rules: {
      'no-restricted-globals': [
        'error',
        ...browserOnlyGlobals.map((name) => ({
          name,
          message: 'Headless package: no DOM or browser APIs.',
        })),
      ],
      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: ['react', 'react-dom', 'react/*', 'react-dom/*'],
              message: 'Headless package: no React.',
            },
          ],
        },
      ],
    },
  },

  {
    files: DETERMINISTIC,
    rules: {
      'no-restricted-properties': [
        'error',
        { object: 'Math', property: 'random', message: nondeterministic },
        { object: 'Date', property: 'now', message: nondeterministic },
        { object: 'performance', property: 'now', message: nondeterministic },
        { object: 'crypto', property: 'getRandomValues', message: nondeterministic },
        { object: 'crypto', property: 'randomUUID', message: nondeterministic },
      ],
      'no-restricted-syntax': [
        'error',
        namedExportsOnly,
        { selector: "NewExpression[callee.name='Date']", message: nondeterministic },
        { selector: "CallExpression[callee.name='Date']", message: nondeterministic },
      ],
    },
  },

  // Node scripts next to a headless package (demo, tooling) may use Node globals such as console.
  {
    files: ['packages/*/scripts/**/*.ts'],
    languageOptions: { globals: globals.node },
    rules: {
      'no-restricted-globals': 'off',
      'no-restricted-properties': 'off',
    },
  },

  // The web app runs in the browser. Timers and performance.now are fine here: the worker host
  // owns wall-clock time; only the engine must stay deterministic.
  {
    files: WEB,
    ignores: WEB_WORKERS,
    languageOptions: { globals: globals.browser },
  },

  // A worker has no DOM: self, postMessage, importScripts, timers, but no window or document.
  // TypeScript checks workers against the DOM lib too (one tsconfig for src/), and no-undef is off
  // for TS, so the DOM-only globals are banned here explicitly.
  {
    files: WEB_WORKERS,
    languageOptions: { globals: globals.worker },
    rules: {
      'no-restricted-globals': [
        'error',
        ...Object.keys(globals.browser)
          .filter((name) => !(name in globals.worker))
          .map((name) => ({ name, message: 'Web Worker: there is no DOM here.' })),
      ],
    },
  },

  {
    files: WEB,
    rules: {
      'no-restricted-syntax': [
        'error',
        namedExportsOnly,
        {
          // Colors come from the ui-spec tokens (CSS variables / Tailwind theme), never literals.
          selector: String.raw`Literal[value=/(^#[0-9a-fA-F]{3,8}$)|(\[#[0-9a-fA-F]{3,8}\])|((rgb|hsl)a?\()/]`,
          message:
            'Hard-coded color. Use a token from docs/ui/ui-spec.md (var(--color-…) or bg-primary etc.).',
        },
      ],
    },
  },

  // Vite and Playwright configs need a default export; e2e specs run in Node.
  {
    files: WEB_NODE,
    languageOptions: { globals: globals.node },
    rules: { 'no-restricted-syntax': 'off' },
  },

  // Config files use default exports by convention; they are plain JS without type info.
  {
    files: ['*.js'],
    extends: [tseslint.configs.disableTypeChecked],
    languageOptions: { globals: globals.node },
    rules: { 'no-restricted-syntax': 'off' },
  },

  prettier,
);
