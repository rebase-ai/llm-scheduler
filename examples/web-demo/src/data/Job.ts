import Task from '@/data/Task';

export enum JobStatus {
  PENDING = 'pending',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export default interface Job {
  id: string;
  task: Task;
  status: JobStatus;
  result: any;
  start_time?: string;
  end_time?: string;
}
