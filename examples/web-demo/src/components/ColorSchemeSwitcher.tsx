'use client';

import { Button } from '@mantine/core';
import { useTheme } from '@/managers/ThemeManager/context';
import LineMdLightDarkLoop from '~icons/line-md/light-dark-loop.jsx';
import LineMdMoonAltLoop from '~icons/line-md/moon-alt-loop.jsx';
import LineMdSunRisingLoop from '~icons/line-md/sun-rising-loop.jsx';

export default function ColorSchemeSwitcher() {
  const { colorScheme, setColorScheme } = useTheme();

  return (
    <div className="flex flex-row gap-4 items-center">
      <Button
        variant={colorScheme === 'auto' ? 'filled' : 'outline'}
        onClick={() => {
          setColorScheme('auto');
        }}
      >
        <LineMdLightDarkLoop />
      </Button>
      <Button
        variant={colorScheme === 'light' ? 'filled' : 'outline'}
        onClick={() => {
          setColorScheme('light');
        }}
      >
        <LineMdSunRisingLoop />
      </Button>
      <Button
        variant={colorScheme === 'dark' ? 'filled' : 'outline'}
        onClick={() => {
          setColorScheme('dark');
        }}
      >
        <LineMdMoonAltLoop />
      </Button>
    </div>
  );
}
