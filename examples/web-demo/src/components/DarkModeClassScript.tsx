import React from 'react';
import { MantineColorScheme } from '@mantine/core';

export interface ColorSchemeScriptProps extends React.ComponentPropsWithoutRef<'script'> {
  forceColorScheme?: 'light' | 'dark';
  defaultColorScheme?: MantineColorScheme;
  localStorageKey?: string;
}

const getScript = ({
  defaultColorScheme,
  localStorageKey,
  forceColorScheme,
}: Pick<ColorSchemeScriptProps, 'defaultColorScheme' | 'localStorageKey' | 'forceColorScheme'>) => {
  if (forceColorScheme === 'light') return '';
  return forceColorScheme
    ? `document.documentElement.classList.add("dark");`
    : `try {
  var _colorScheme = window.localStorage.getItem("${localStorageKey}");
  var colorScheme = _colorScheme === "light" || _colorScheme === "dark" || _colorScheme === "auto" ? _colorScheme : "${defaultColorScheme}";
  var computedColorScheme = colorScheme !== "auto" ? colorScheme : window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  computedColorScheme === "dark" && document.documentElement.classList.add("dark");
} catch (e) {}
`;
};

export function DarkModeClassScript({
  defaultColorScheme = 'light',
  localStorageKey = 'mantine-color-scheme-value',
  forceColorScheme,
  ...others
}: ColorSchemeScriptProps) {
  const _defaultColorScheme = ['light', 'dark', 'auto'].includes(defaultColorScheme) ? defaultColorScheme : 'light';
  return (
    <script
      {...others}
      dangerouslySetInnerHTML={{
        __html: getScript({
          defaultColorScheme: _defaultColorScheme,
          localStorageKey,
          forceColorScheme,
        }),
      }}
    />
  );
}
