import pluginVue from 'eslint-plugin-vue'
import parserVue from 'vue-eslint-parser'
import configPrettier from 'eslint-config-prettier'
import tseslint from 'typescript-eslint'

export default [
  ...tseslint.configs.recommended,
  {
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },
  configPrettier,
  {
    name: 'app/vue-files',
    files: ['**/*.vue'],
    languageOptions: {
      parser: parserVue,
      ecmaVersion: 'latest',
      sourceType: 'module',
      parserOptions: {
        parser: tseslint.parser,
      },
    },
    plugins: {
      vue: pluginVue,
    },
    processor: pluginVue.processors['.vue'],
    rules: {
      ...pluginVue.configs['flat/essential'].rules,
      ...pluginVue.configs['flat/recommended'].rules,
      ...pluginVue.configs['flat/strongly-recommended'].rules,
    },
  },
]
