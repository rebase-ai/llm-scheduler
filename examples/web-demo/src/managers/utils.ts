import { getThemePersistentStateFromCookies } from '@/managers/ThemeManager/utils';

export async function getManagersPersistentStateFromCookies() {
  return {
    theme: await getThemePersistentStateFromCookies(),
  };
}
