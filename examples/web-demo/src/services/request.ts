import axios from 'axios';

export const SERVER_SIDE_HOST =
  process.env.HOSTNAME && process.env.PORT ? `${process.env.HOSTNAME}:${process.env.PORT}` : 'localhost:3000';

const request = axios.create({
  baseURL: typeof window === 'undefined' ? `http://${SERVER_SIDE_HOST}/api` : '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

export default request;
