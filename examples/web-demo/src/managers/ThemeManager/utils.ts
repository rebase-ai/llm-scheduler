import { cookies } from 'next/headers';
import {
  THEME_COLOR_SCHEMA_STORE_NAME,
  THEME_DIRECTION_STORE_NAME,
  THEME_PRIMARY_COLOR_STORE_NAME,
  ThemeColorScheme,
  ThemeDirection,
} from './def';

export async function getThemePersistentStateFromCookies() {
  const cookieStore = await cookies();

  const colorScheme = (cookieStore.get(THEME_COLOR_SCHEMA_STORE_NAME)?.value || 'dark') as ThemeColorScheme;
  const direction = cookieStore.get(THEME_DIRECTION_STORE_NAME)?.value as ThemeDirection;
  const primaryColor = cookieStore.get(THEME_PRIMARY_COLOR_STORE_NAME)?.value as string;

  return {
    colorScheme,
    direction,
    primaryColor,
  };
}
