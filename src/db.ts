import { open, Database } from "sqlite";
import sqlite3 from "sqlite3";
import fs from "fs";

// Open the database and init tables if required
export async function openDb(): Promise<
  Database<sqlite3.Database, sqlite3.Statement>
> {
  const db = await open({
    filename: "events.db",
    driver: sqlite3.Database,
  });

  await db.run(`CREATE TABLE IF NOT EXISTS events (
    block_number INTEGER,
    event_type TEXT,
    amount INTEGER,
    who TEXT,
    PRIMARY KEY (block_number, who, event_type)
  )`);

  await db.run(`CREATE TABLE IF NOT EXISTS last_processed_block (
    block_number INTEGER PRIMARY KEY
  )`);

  return db;
}

export async function insertEvent(
  db: Database<sqlite3.Database, sqlite3.Statement>,
  blockNumber: number,
  eventType: string,
  amount: number,
  who: string,
): Promise<void> {
  await db.run(
    `INSERT OR REPLACE INTO events (block_number, event_type, amount, who) VALUES (?, ?, ?, ?)`,
    [blockNumber, eventType, amount, who],
  );
}

export async function setLastProcessedBlock(
  db: Database<sqlite3.Database, sqlite3.Statement>,
  blockNumber: number,
): Promise<void> {
  await db.run(
    `INSERT OR REPLACE INTO last_processed_block (block_number) VALUES (?)`,
    [blockNumber],
  );
}

export async function getLastProcessedBlock(
  db: Database<sqlite3.Database, sqlite3.Statement>,
): Promise<number> {
  const row = await db.get<{ block_number: number }>(
    `SELECT block_number FROM last_processed_block`,
  );
  // this is the first block number with the electionsPhragmen event on Polkadot
  return row ? row.block_number : 746096;
}

export async function calculateNetReserves(): Promise<void> {
  const db = await openDb();

  try {
    const rows = await db.all(`
      SELECT who, 
             SUM(CASE WHEN event_type = 'reserve' THEN amount ELSE 0 END) - 
             SUM(CASE WHEN event_type = 'unreserve' THEN amount ELSE 0 END) AS net_reserve
      FROM events
      GROUP BY who
    `);

    rows.forEach((row) => {
      console.log(`Address: ${row.who}, Net Reserve: ${row.net_reserve}`);
    });
  } catch (error) {
    console.error("Error while calculating net reserves:", error);
  } finally {
    await db.close();
  }
}

export interface NetReserveRow {
  who: string;
  net_reserve: number;
}

export async function fetchNetReserves(
  db: Database<sqlite3.Database, sqlite3.Statement>,
): Promise<NetReserveRow[]> {
  try {
    return await db.all<NetReserveRow[]>(`
      SELECT who, 
             SUM(CASE WHEN event_type = 'reserve' THEN amount ELSE 0 END) - 
             SUM(CASE WHEN event_type = 'unreserve' THEN amount ELSE 0 END) AS net_reserve
      FROM events
      GROUP BY who
    `);
  } catch (error) {
    console.error("Error while fetching net reserves:", error);
    throw error; // rethrow the error to be handled by the caller
  } finally {
    await db.close();
  }
}

export async function writeToCSV(
  rows: NetReserveRow[],
  filePath: string,
): Promise<void> {
  const writeStream = fs.createWriteStream(filePath);
  writeStream.write("Address,Net Reserve\n");

  rows.forEach((row) => {
    writeStream.write(`${row.who},${row.net_reserve}\n`);
  });

  writeStream.end();
}
