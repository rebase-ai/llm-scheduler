import { PROJECT_STORE_PREFIX } from '@/project.config';
import { DefaultMantineColor, Direction, MantineColorScheme, MantineTheme } from '@mantine/core';

export const THEME_COLOR_SCHEMA_STORE_NAME = `${PROJECT_STORE_PREFIX}_THEME_COLOR_SCHEMA`;
export const THEME_DIRECTION_STORE_NAME = `${PROJECT_STORE_PREFIX}_THEME_DIRECTION`;
export const THEME_PRIMARY_COLOR_STORE_NAME = `${PROJECT_STORE_PREFIX}_THEME_PRIMARY_COLOR`;

export type ThemeColorScheme = MantineColorScheme;
export type ThemeActiveColorScheme = ThemeColorScheme;
export type ThemeDirection = Direction;
export type ThemePrimaryColor = DefaultMantineColor;

export interface ThemeContextValues {
  colorScheme: ThemeColorScheme;
  activeColorScheme: Exclude<ThemeActiveColorScheme, 'auto'>;
  direction: ThemeDirection;
  primaryColor: ThemePrimaryColor;

  isDark: boolean;
  colorPrimaryShade: number;
  theme: MantineTheme;

  toggleColorScheme: () => void;
  setColorScheme: (colorScheme: ThemeColorScheme) => void;
  clearColorScheme: () => void;
  toggleDirection: () => void;
  setDirection: (direction: ThemeDirection) => void;
  setPrimaryColor: (primaryColor: string) => void;
}
