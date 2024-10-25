import Job from '@/data/Job';
import Task from '@/data/Task';
import request from '@/services/request';

export interface TaskWithJobs extends Task {
  jobs: Job[];
}

const APIService = {
  async getTaskJobs(taskId: string): Promise<Job[]> {
    return (await request.get(`/tasks/${taskId}/jobs`)).data.jobs;
  },
  async getTasksWithJobs(): Promise<TaskWithJobs[]> {
    const tasks = (await request.get('/tasks')).data.tasks;
    return Promise.all(
      tasks.map(async (task: Task) => {
        const jobs = await this.getTaskJobs(task.id);
        return { ...task, jobs };
      })
    );
  },
};

export default APIService;
