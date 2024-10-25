import React from 'react';
import type { Metadata } from 'next';
import { getLocale } from 'next-intl/server';
import { ColorSchemeScript } from '@mantine/core';
import '@/styles/global.scss';
import ManagersRegistry from '@/managers/ManagersRegistry';
import { THEME_COLOR_SCHEMA_STORE_NAME } from '@/managers/ThemeManager/def';
import { getManagersPersistentStateFromCookies } from '@/managers/utils';
import { DarkModeClassScript } from '@/components/DarkModeClassScript';

export const metadata: Metadata = {
  title: 'LLM Scheduler Demo',
  description: 'LLM Scheduler Demo',
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const locale = await getLocale();
  const managersPersistentState = await getManagersPersistentStateFromCookies();

  return (
    <html lang={locale} dir={managersPersistentState.theme.direction} suppressHydrationWarning>
      <head>
        <meta name="viewport" content="minimum-scale=1, initial-scale=1, width=device-width, user-scalable=no" />
        <meta name="theme-color" />
        <ColorSchemeScript
          defaultColorScheme={managersPersistentState.theme.colorScheme}
          localStorageKey={THEME_COLOR_SCHEMA_STORE_NAME}
        />
        <DarkModeClassScript
          defaultColorScheme={managersPersistentState.theme.colorScheme}
          localStorageKey={THEME_COLOR_SCHEMA_STORE_NAME}
        />
      </head>
      <body className="antialiased">
        <ManagersRegistry persistentState={managersPersistentState}>{children}</ManagersRegistry>
      </body>
    </html>
  );
}
