import { Hono } from 'hono';
import { logger } from 'hono/logger';
import { handle } from 'hono/vercel';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const proxyServer = new Hono().basePath('/api');
proxyServer.use(logger());

const serverURL = 'http://127.0.0.1:8000';
proxyServer.all('*', async (c) => {
  const { path } = c.req;
  let url = serverURL ? serverURL + path.replace(/^\/api/, '') : c.req.url;
  if (c.req.query()) url = `${url}?${new URLSearchParams(c.req.query())}`;
  const rep = await fetch(url, {
    method: c.req.method,
    headers: c.req.raw.clone().headers,
    body: c.req.raw.body,
    ...(c.req.raw.body ? { duplex: 'half' } : {}),
  });
  return c.newResponse(rep.body, rep.status as any, Object.fromEntries(rep.headers));
});

export const GET = handle(proxyServer) as any;
export const POST = handle(proxyServer) as any;
export const PUT = handle(proxyServer) as any;
export const PATCH = handle(proxyServer) as any;
export const DELETE = handle(proxyServer) as any;
export const HEAD = handle(proxyServer) as any;
export const OPTIONS = handle(proxyServer) as any;
