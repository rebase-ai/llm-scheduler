'use client';

import { PropsWithChildren, createContext, useContext, useMemo } from 'react';
import { useMantineTheme } from '@mantine/core';
import { ThemeContextValues } from './def';

const ThemeContext = createContext<ThemeContextValues | null>(null);

export function useTheme() {
  const context = useContext(ThemeContext);

  if (!context) {
    throw new Error(
      'useTheme hook was called outside of context, make sure your app is wrapped with ThemeProvider component'
    );
  }

  return context;
}

interface ThemeProviderProps
  extends Omit<ThemeContextValues, 'isDark' | 'colorPrimaryShade' | 'theme'>,
    PropsWithChildren {}

const ThemeProvider = ({
  colorScheme,
  activeColorScheme,
  direction,
  primaryColor,
  toggleColorScheme,
  setColorScheme,
  clearColorScheme,
  toggleDirection,
  setDirection,
  setPrimaryColor,
  children,
}: ThemeProviderProps) => {
  const isDark = activeColorScheme === 'dark';
  const theme = useMantineTheme();
  const colorPrimaryShade = useMemo(() => {
    const { primaryShade } = theme;
    if (typeof primaryShade === 'number') {
      return primaryShade;
    }
    return isDark ? primaryShade.dark : primaryShade.light;
  }, [theme, isDark]);

  return (
    <ThemeContext.Provider
      value={{
        colorScheme,
        activeColorScheme,
        direction,
        primaryColor,
        isDark,
        colorPrimaryShade,
        theme,
        toggleColorScheme,
        setColorScheme,
        clearColorScheme,
        toggleDirection,
        setDirection,
        setPrimaryColor,
      }}
    >
      {children}
    </ThemeContext.Provider>
  );
};

export default ThemeProvider;
