import React, { useEffect } from 'react';
import '../src/styles/global.scss';
import { DARK_MODE_EVENT_NAME } from 'storybook-dark-mode';
import { addons } from '@storybook/preview-api';
import type { Preview } from '@storybook/react';
import ManagersRegistry from '../src/managers/ManagersRegistry';
import { useTheme } from '../src/managers/ThemeManager/context';

const channel = addons.getChannel();

function ColorSchemeWrapper({ children }: { children: React.ReactNode }) {
  const { setColorScheme } = useTheme();
  const handleColorScheme = (value: boolean) => setColorScheme(value ? 'dark' : 'light');

  useEffect(() => {
    channel.on(DARK_MODE_EVENT_NAME, handleColorScheme);
    return () => channel.off(DARK_MODE_EVENT_NAME, handleColorScheme);
  }, [channel]);

  return <>{children}</>;
}

export const decorators = [
  (renderStory: any) => <ColorSchemeWrapper>{renderStory()}</ColorSchemeWrapper>,
  (renderStory: any) => (
    <ManagersRegistry
      persistentState={{
        theme: {
          colorScheme: 'light',
          direction: 'ltr',
          primaryColor: 'teal',
        },
      }}
    >
      {' '}
      {renderStory()}
    </ManagersRegistry>
  ),
];

const preview: Preview = {
  parameters: {
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
  },
};

export default preview;
