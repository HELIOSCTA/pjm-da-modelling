import "server-only";

import { Pool, type PoolConfig } from "pg";

declare global {
  var __pgPool: Pool | undefined;
}

function buildConfig(): PoolConfig {
  const url = process.env.DATABASE_URL;
  if (url) {
    return {
      connectionString: url,
      ssl: { rejectUnauthorized: false },
      max: 4,
      idleTimeoutMillis: 30_000,
    };
  }

  const host = process.env.AZURE_POSTGRESQL_DB_HOST;
  const user = process.env.AZURE_POSTGRESQL_DB_USER;
  const password = process.env.AZURE_POSTGRESQL_DB_PASSWORD;
  const database = process.env.AZURE_POSTGRESQL_DB_NAME;
  const port = process.env.AZURE_POSTGRESQL_DB_PORT;

  if (!host || !user || !password || !database) {
    throw new Error(
      "Postgres env missing. Set DATABASE_URL or the AZURE_POSTGRESQL_DB_* vars in frontend/.env.local.",
    );
  }

  return {
    host,
    user,
    password,
    database,
    port: port ? Number(port) : 5432,
    ssl: { rejectUnauthorized: false },
    max: 4,
    idleTimeoutMillis: 30_000,
  };
}

export function getPool(): Pool {
  if (!global.__pgPool) {
    global.__pgPool = new Pool(buildConfig());
  }
  return global.__pgPool;
}

export async function query<T>(
  text: string,
  values?: ReadonlyArray<unknown>,
): Promise<T[]> {
  const pool = getPool();
  const res = await pool.query(text, values as unknown[] | undefined);
  return res.rows as T[];
}
