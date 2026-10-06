// Collector lint tooling is pinned and isolated; no dependency or user-settings mutation.
import { createRequire } from 'node:module';
import path from 'node:path';

if (!process.env.PLIMSOLL_LINT_TOOLS) throw new Error('PLIMSOLL_LINT_TOOLS_required');
const require = createRequire(path.resolve(process.env.PLIMSOLL_LINT_TOOLS, 'package.json'));
const tseslint = require('typescript-eslint');
export default tseslint.config(...tseslint.configs.recommended, {
  files: ['**/*.ts'],
  rules: {
    '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
    // Existing third-party boundary casts and CJS interoperability are allowed.
    '@typescript-eslint/no-explicit-any': 'off',
    '@typescript-eslint/no-require-imports': 'off',
    'no-empty': ['error', { allowEmptyCatch: true }],
  },
});
