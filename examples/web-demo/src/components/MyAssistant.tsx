'use client';

import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import TaskItem from '@/components/TaskItem';
import { AnimatedListItem } from '@/components/ui/animated-list';
import useMemoizedFn from '@/hooks/useMemoizedFn';
import APIService, { TaskWithJobs } from '@/services';
import { Thread, useThread } from '@assistant-ui/react';
import { makeMarkdownText } from '@assistant-ui/react-markdown';

const MarkdownText = makeMarkdownText();

const ThreadContainer = styled.div`
  width: 70%;
  height: 100%;
  overflow: hidden;
  position: relative;
`;

const TasksArea = styled.div`
  width: 30%;
  height: 100%;
  position: relative;
  border-left: 1px solid rgb(180, 180, 180);
  overflow-y: auto;
  overflow-x: hidden;
  padding: 20px;

  .syncIndicator {
    opacity: 0.35;
    pointer-events: none;
    transform-origin: center;
    animation: spinAndPause 2s infinite linear;
  }
`;

const Container = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: stretch;
  overflow: hidden;
  position: relative;
  background-color: hsl(var(--aui-background));
`;

export function MyAssistant() {
  const isRunning = useThread((m) => m.isRunning);
  const [tasks, setTasks] = useState<TaskWithJobs[]>([]);

  const loadingRef = useRef(false);
  const autoRefreshTimerRef = useRef<ReturnType<typeof setInterval>>(undefined);

  const loadTasks = useMemoizedFn(async () => {
    if (loadingRef.current) return;
    try {
      const newTasks = await APIService.getTasksWithJobs();
      setTasks(newTasks);
    } catch (e) {
      console.error(e);
    } finally {
      loadingRef.current = false;
    }
  });

  const startAutoRefresh = useMemoizedFn(() => {
    if (autoRefreshTimerRef.current) return;
    autoRefreshTimerRef.current = setInterval(() => {
      loadTasks();
    }, 3000);
  });

  const stopAutoRefresh = useMemoizedFn(() => {
    if (autoRefreshTimerRef.current) {
      clearInterval(autoRefreshTimerRef.current);
      autoRefreshTimerRef.current = undefined;
    }
  });

  const lastIsRunning = useRef(isRunning);
  useEffect(() => {
    if (lastIsRunning.current !== isRunning && !isRunning) {
      loadTasks();
    }
    lastIsRunning.current = isRunning;
  }, [isRunning]);

  useEffect(() => {
    loadTasks();
    startAutoRefresh();

    return () => {
      stopAutoRefresh();
    };
  }, []);

  return (
    <Container>
      <ThreadContainer>
        <Thread assistantMessage={{ components: { Text: MarkdownText } }} />
      </ThreadContainer>
      <TasksArea>
        <AnimatedListItem>
          {tasks.map((task) => (
            <TaskItem key={task.id} task={task} />
          ))}
        </AnimatedListItem>
      </TasksArea>
    </Container>
  );
}
