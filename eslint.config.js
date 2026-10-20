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

export default defineConfig(
  { ignores: ['**/node_modules/**', '**/dist/**', '**/coverage/**'] },

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
      'no-restricted-syntax': [
        'error',
        { selector: 'ExportDefaultDeclaration', message: 'Use named exports.' },
      ],
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
        { selector: 'ExportDefaultDeclaration', message: 'Use named exports.' },
        { selector: "NewExpression[callee.name='Date']", message: nondeterministic },
        { selector: "CallExpression[callee.name='Date']", message: nondeterministic },
      ],
    },
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
