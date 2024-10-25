import { Schedule } from '@/data/Schedule';

export default interface Task {
  id: string;
  name: string;
  description?: string;
  created_at: string;
  schedule: Schedule;
  payload: Record<string, any>;
  payload_schema_name: string;
  meta?: Record<string, any>;
  is_active: boolean;
}
