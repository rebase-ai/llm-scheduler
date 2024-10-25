import { useState } from 'react';
import { Card, Flex, Text } from '@mantine/core';
import styled from '@emotion/styled';
import dayjs from 'dayjs';
import { ScheduleType } from '@/data/Schedule';
import { AnimatedListItem } from '@/components/ui/animated-list';
import ShineBorder from '@/components/ui/shine-border';
import { MdAccessTime } from 'react-icons/md';
import MdiTimerPlayOutline from '~icons/mdi/timer-play-outline.jsx';
import MdiTimerSyncOutline from '~icons/mdi/timer-sync-outline.jsx';
import RiTimerFlashLine from '~icons/ri/timer-flash-line.jsx';
import { TaskWithJobs } from '@/services';

const Container = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: stretch;
  flex-shrink: 0;
  position: relative;
  margin-bottom: 12px;
`;

const Content = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: stretch;
  flex-shrink: 0;
  font-size: 14px;
  background: var(--mantine-color-dark-6);

  .itemHeader {
    height: 45px;
    padding: 6px 0;
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
    align-items: center;
    gap: 10px;
    flex-shrink: 0;
    overflow: hidden;
    z-index: 11;
    font-size: 18px;
  }

  .itemContent {
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    align-items: stretch;
    flex-shrink: 0;
  }

  .itemFooter {
    height: 35px;
    display: flex;
    flex-direction: row;
    justify-content: flex-end;
    align-items: center;
    flex-shrink: 0;
    font-size: 12px;
  }
`;

export interface TaskItemProps {
  task: TaskWithJobs;
}

const TaskItem = ({ task }: TaskItemProps) => {
  const [expanded, setExpanded] = useState(false);

  return (
    <Container>
      <ShineBorder
        className={`z-10 flex flex-col justify-start items-stretch w-full relative flex-shrink-0 overflow-hidden mb-[6px]  ${task.is_active ? 'p-[1px]' : 'p-0'}`}
        color={['var(--mantine-color-primary-6)', '#A07CFE', '#FE8FB5']}
      >
        <Card
          styles={{
            root: {
              padding: '0 12px 6px 12px',
              borderRadius: 'var(--border-radius)',
            },
          }}
        >
          <Content>
            <div className="itemHeader">
              {task.schedule.type === ScheduleType.IMMEDIATE && (
                <RiTimerFlashLine color="var(--mantine-color-primary-6)" />
              )}
              {task.schedule.type === ScheduleType.ONE_TIME && (
                <MdiTimerPlayOutline color="var(--mantine-color-primary-6)" />
              )}
              {task.schedule.type === ScheduleType.RECURRING && (
                <MdiTimerSyncOutline color="var(--mantine-color-primary-6)" />
              )}
              {task.name}
            </div>
            <div className="itemContent whitespace-pre-wrap">
              <Text>Payload Schema: {task.payload_schema_name}</Text>
              <Text className="whitespace-pre-wrap">{`Payload: \n${JSON.stringify(task.payload, null, 4)
                .split('\n')
                .map((l) => `         ${l}`)
                .join('\n')}`}</Text>
              <Text className="whitespace-pre-wrap">{`Schedule: \n${JSON.stringify(task.schedule, null, 4)
                .split('\n')
                .map((l) => `         ${l}`)
                .join('\n')}`}</Text>
            </div>
            <div className="itemFooter">
              <Flex align="center">
                <MdAccessTime />
                <Text ml={4} inherit className="tabular-nums">
                  {dayjs(task.created_at).format('YYYY-MM-DD HH:mm:ss')}
                </Text>
              </Flex>
            </div>
          </Content>
        </Card>
      </ShineBorder>
      <AnimatedListItem>
        {task.jobs.slice(0, expanded ? task.jobs.length : 3).map((job) => (
          <Card
            key={job.id}
            styles={{
              root: {
                marginBottom: '6px',
              },
            }}
          >
            <Flex direction="column" justify="flex-start" align="stretch">
              {job.result.message}
              <Flex align="center" className="self-end text-[12px]">
                <MdAccessTime />
                <Text ml={4} inherit className="tabular-nums">
                  {dayjs(job.start_time).format('YYYY-MM-DD HH:mm:ss')}
                </Text>
              </Flex>
            </Flex>
          </Card>
        ))}
        {task.jobs.length > 3 && (
          <Flex
            justify="center"
            className="cursor-pointer"
            onClick={() => {
              setExpanded(!expanded);
            }}
          >
            <Text size="xs" className="tabular-nums">
              {expanded ? 'Show less' : `Show ${task.jobs.length - 3} more`}
            </Text>
          </Flex>
        )}
      </AnimatedListItem>
    </Container>
  );
};

export default TaskItem;
