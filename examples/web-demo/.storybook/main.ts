import Icons from 'unplugin-icons/webpack';
import type { StorybookConfig } from '@storybook/nextjs';

const config: StorybookConfig = {
  stories: ['../src/**/*.mdx', '../src/**/*.stories.@(js|jsx|mjs|ts|tsx)'],
  addons: [
    '@storybook/addon-onboarding',
    '@storybook/addon-links',
    '@storybook/addon-essentials',
    '@chromatic-com/storybook',
    '@storybook/addon-interactions',
    '@storybook/addon-styling-webpack',
    'storybook-dark-mode',
  ],
  framework: {
    name: '@storybook/nextjs',
    options: {},
  },
  staticDirs: ['../public'],
  features: {
    experimentalRSC: true,
  },
  webpackFinal: async (config) => {
    config.plugins?.push(
      Icons({
        compiler: 'jsx',
        jsx: 'react',
      })
    );
    return config;
  },
};
export default config;
