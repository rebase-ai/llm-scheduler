'use client';

import { PropsWithChildren } from 'react';
import { PROJECT_STORE_PREFIX } from '@/project.config';
import { prefixer } from 'stylis';
import EmotionCacheProvider from './EmotionCache';

const StyleRegistry = ({ children }: PropsWithChildren) => {
  return (
    <EmotionCacheProvider
      options={{
        key: PROJECT_STORE_PREFIX.toLowerCase().replace(/[^a-z]/g, ''),
        stylisPlugins: [prefixer],
      }}
    >
      {children}
    </EmotionCacheProvider>
  );
};

export default StyleRegistry;
