'use client';

import { PropsWithChildren, useEffect, useMemo, useState } from 'react';
import { generateColorsMap } from '@mantine/colors-generator';
import {
  DEFAULT_THEME,
  DirectionProvider,
  MantineColorShade,
  MantineColorsTuple,
  MantinePrimaryShade,
  MantineProvider,
  localStorageColorSchemeManager,
  useComputedColorScheme,
  useDirection,
  useMantineColorScheme,
} from '@mantine/core';
import { ModalsProvider } from '@mantine/modals';
import { Notifications } from '@mantine/notifications';
import dayjs from 'dayjs';
import Cookies from 'js-cookie';
import StyleRegistry from './StyleRegistry';
import ThemeProvider from './context';
import {
  THEME_COLOR_SCHEMA_STORE_NAME,
  THEME_DIRECTION_STORE_NAME,
  THEME_PRIMARY_COLOR_STORE_NAME,
  ThemeContextValues,
} from './def';

const cookieOptions: Cookies.CookieAttributes = {
  expires: dayjs().add(3, 'year').toDate(),
};

const colorSchemeManager = localStorageColorSchemeManager({
  key: THEME_COLOR_SCHEMA_STORE_NAME,
});

interface ThemeManagerStateProviderProps
  extends PropsWithChildren,
    Pick<ThemeContextValues, 'colorScheme' | 'direction' | 'primaryColor'> {
  onPrimaryColorChange: (newPrimaryColor: string) => void;
}

function ThemeManagerStateProvider({ children, ...props }: ThemeManagerStateProviderProps) {
  const { setColorScheme: mantineSetColorScheme, clearColorScheme: mantineClearColorScheme } = useMantineColorScheme();
  const activeColorScheme = useComputedColorScheme();
  const { dir, setDirection: mantineSetDirection } = useDirection();

  const [colorScheme, setColorScheme] = useState(props.colorScheme);
  const [direction, setDirection] = useState(props.direction);

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const divElement = document.createElement('div');
      divElement.style.display = 'none';
      divElement.style.color = 'var(--mantine-color-body)';
      document.body.appendChild(divElement);
      const bodyColor = window.getComputedStyle(divElement).color;
      divElement.remove();
      const themeColorMeta = document.querySelector('meta[name="theme-color"]');
      if (themeColorMeta) {
        themeColorMeta.setAttribute('content', bodyColor);
      }
      try {
        if (activeColorScheme === 'dark') {
          document.documentElement.classList.add('dark');
        } else {
          document.documentElement.classList.remove('dark');
        }
      } catch {}
    }
  }, [activeColorScheme]);

  return (
    <ThemeProvider
      colorScheme={colorScheme}
      activeColorScheme={activeColorScheme}
      direction={direction}
      primaryColor={props.primaryColor}
      toggleColorScheme={() => {
        const targetCS = activeColorScheme === 'light' ? 'dark' : 'light';
        setColorScheme(targetCS);
        Cookies.set(THEME_COLOR_SCHEMA_STORE_NAME, targetCS, cookieOptions);
        mantineSetColorScheme(targetCS);
      }}
      setColorScheme={(newColorScheme) => {
        setColorScheme(newColorScheme);
        Cookies.set(THEME_COLOR_SCHEMA_STORE_NAME, newColorScheme, cookieOptions);
        mantineSetColorScheme(newColorScheme);
      }}
      clearColorScheme={() => {
        setColorScheme('light');
        Cookies.remove(THEME_COLOR_SCHEMA_STORE_NAME);
        mantineClearColorScheme();
      }}
      toggleDirection={() => {
        const targetDir = dir === 'ltr' ? 'rtl' : 'ltr';
        setDirection(targetDir);
        Cookies.set(THEME_DIRECTION_STORE_NAME, targetDir, cookieOptions);
        mantineSetDirection(targetDir);
      }}
      setDirection={(newDirection) => {
        setDirection(newDirection);
        Cookies.set(THEME_DIRECTION_STORE_NAME, newDirection, cookieOptions);
        mantineSetDirection(newDirection);
      }}
      setPrimaryColor={(newPrimaryColor) => {
        Cookies.set(THEME_PRIMARY_COLOR_STORE_NAME, newPrimaryColor, cookieOptions);
        props.onPrimaryColorChange(newPrimaryColor);
      }}
    >
      {children}
    </ThemeProvider>
  );
}

interface ThemeManagerRegistryProps
  extends PropsWithChildren,
    Pick<ThemeContextValues, 'colorScheme' | 'direction' | 'primaryColor'> {}

const ThemeManagerRegistry = ({ children, ...props }: ThemeManagerRegistryProps) => {
  const [primaryColor, setPrimaryColor] = useState(props.primaryColor || 'teal');

  const {
    colors,
    baseColorIndex,
  }: {
    colors: MantineColorsTuple | undefined;
    baseColorIndex: MantineColorShade | MantinePrimaryShade | undefined;
  } = useMemo(() => {
    if (primaryColor.startsWith('#')) {
      const result = generateColorsMap(primaryColor);
      return {
        colors: result.colors.map((color) => color.hex()) as never as MantineColorsTuple,
        baseColorIndex: result.baseColorIndex as MantineColorShade,
      };
    }
    return {
      colors: DEFAULT_THEME.colors[primaryColor],
      baseColorIndex: DEFAULT_THEME.primaryShade,
    };
  }, [primaryColor]);

  return (
    <StyleRegistry>
      <DirectionProvider initialDirection={props.direction}>
        <MantineProvider
          defaultColorScheme={props.colorScheme}
          colorSchemeManager={colorSchemeManager}
          theme={{
            ...(colors
              ? {
                  colors: { primary: colors },
                  primaryColor: 'primary',
                  primaryShade: baseColorIndex,
                }
              : {}),
            components: {
              Modal: {
                styles: {
                  header: {
                    minHeight: 55,
                    padding: '0 var(--mantine-spacing-md)',
                  },
                },
              },
            },
          }}
        >
          <ThemeManagerStateProvider
            colorScheme={props.colorScheme}
            direction={props.direction}
            primaryColor={primaryColor}
            onPrimaryColorChange={(newPrimaryColor) => {
              setPrimaryColor(newPrimaryColor);
            }}
          >
            <Notifications autoClose={4000} position="top-right" />
            <ModalsProvider
              modalProps={{
                centered: true,
              }}
            >
              {children}
            </ModalsProvider>
          </ThemeManagerStateProvider>
        </MantineProvider>
      </DirectionProvider>
    </StyleRegistry>
  );
};

export default ThemeManagerRegistry;
