export enum ScheduleType {
  IMMEDIATE = 'immediate',
  ONE_TIME = 'one_time',
  RECURRING = 'recurring',
}

export interface Schedule {
  type: ScheduleType;
  description?: string;
}

export interface ImmediateSchedule extends Schedule {
  type: ScheduleType.IMMEDIATE;
}

export interface OneTimeSchedule extends Schedule {
  type: ScheduleType.ONE_TIME;
  execution_time: string;
}

export interface RecurringSchedule extends Schedule {
  type: ScheduleType.RECURRING;
  cron_expression: string;
  start_time?: string;
  end_time?: string;
}
