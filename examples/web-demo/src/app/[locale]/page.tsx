'use client';

import axios from 'axios';
import { MyAssistant } from '@/components/MyAssistant';
import {
  AssistantRuntimeProvider,
  ChatModelAdapter,
  TextContentPart,
  ThreadUserMessage,
  useLocalRuntime,
} from '@assistant-ui/react';

const MyModelAdapter: ChatModelAdapter = {
  async run({ messages, abortSignal }) {
    const userMessage = messages[messages.length - 1] as ThreadUserMessage;
    const result = await axios.post<{
      response: string;
    }>(
      '/api/chat',
      {
        message: userMessage.content
          .map((part) => (part.type === 'text' ? (part as TextContentPart).text : undefined))
          .filter((p) => !!p)
          .join('\n'),
      },
      { signal: abortSignal }
    );

    return {
      content: [
        {
          type: 'text',
          text: result.data.response,
        },
      ],
    };
  },
};

export default function Home() {
  const runtime = useLocalRuntime(MyModelAdapter);

  return (
    <main className="h-dvh">
      <AssistantRuntimeProvider runtime={runtime}>
        <MyAssistant />
      </AssistantRuntimeProvider>
    </main>
  );
}
