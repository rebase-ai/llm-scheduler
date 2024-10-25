import type { NextConfig } from 'next';
import createNextIntlPlugin from 'next-intl/plugin';
import Icons from 'unplugin-icons/webpack';
import bundleAnalyzer from '@next/bundle-analyzer';

const withNextIntl = createNextIntlPlugin();

const withBundleAnalyzer = bundleAnalyzer({
  enabled: process.env.ANALYZE === 'true',
});

const nextConfig: NextConfig = {
  compiler: {
    emotion: true,
  },
  sassOptions: {
    includePaths: ['src', 'node_modules'],
  },
  experimental: {
    optimizePackageImports: ['@mantine/core', '@mantine/hooks', '@mantine/modals', '@mantine/notifications'],
  },
  webpack(config) {
    // Add the unplugin-icons plugin to the webpack config
    config.plugins.push(
      Icons({
        compiler: 'jsx',
        jsx: 'react',
        autoInstall: true,
      })
    );

    return config;
  },
  output: process.env.BUILD_FOR_DOCKER === 'true' ? 'standalone' : undefined,
};

export default withBundleAnalyzer(withNextIntl(nextConfig));
