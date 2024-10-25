import type { Meta, StoryObj } from '@storybook/react';
import ColorSchemeSwitcher from './ColorSchemeSwitcher';

const meta = {
  component: ColorSchemeSwitcher,
} satisfies Meta<typeof ColorSchemeSwitcher>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
