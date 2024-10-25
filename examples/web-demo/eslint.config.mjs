import path from 'node:path';
import { fileURLToPath } from 'node:url';
import js from '@eslint/js';
import { FlatCompat } from '@eslint/eslintrc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended,
  allConfig: js.configs.all,
});

const configs = [
  ...compat.extends(
    'next/core-web-vitals',
    'next/typescript',
    'plugin:next-unplugin-icons/recommended',
    'plugin:storybook/recommended',
    'prettier'
  ),
  ...compat.plugins('react-compiler'),
  {
    rules: {
      'react-compiler/react-compiler': 'error',
      // Consistently import navigation APIs from `@/i18n/routing`
      'no-restricted-imports': [
        'error',
        {
          name: 'next/link',
          message: 'Please import from `@/i18n/routing` instead.',
        },
        {
          name: 'next/navigation',
          importNames: ['redirect', 'permanentRedirect', 'useRouter', 'usePathname'],
          message: 'Please import from `@/i18n/routing` instead.',
        },
      ],
    },
  },
  {
    rules: {
      'react/no-unescaped-entities': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },
];

export default configs;
