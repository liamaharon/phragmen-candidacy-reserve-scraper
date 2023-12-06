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
  amount: string,
  who: string,
): Promise<void> {
  console.log("insert", blockNumber, eventType, amount, who);
  await db.run(
    `INSERT OR REPLACE INTO events (block_number, event_type, amount, who) VALUES (?, ?, ?, ?)`,
    [blockNumber, eventType, amount, who],
  );
}

export async function setLastProcessedBlock(
  db: Database<sqlite3.Database, sqlite3.Statement>,
  blockNumber: number,
): Promise<void> {
  // Clear the existing entry
  await db.run(`DELETE FROM last_processed_block`);

  // Insert the new block number
  await db.run(`INSERT INTO last_processed_block (block_number) VALUES (?)`, [
    blockNumber,
  ]);
}

export async function getLastProcessedBlock(
  db: Database<sqlite3.Database, sqlite3.Statement>,
  default_block_number: number,
): Promise<number> {
  const row = await db.get<{ block_number: number }>(
    `SELECT block_number FROM last_processed_block`,
  );
  return row ? row.block_number : default_block_number;
}

export interface NetReserveRow {
  who: string;
  net_reserve: number;
}

export function fetchNetReserves(
  db: Database<sqlite3.Database, sqlite3.Statement>,
): Promise<NetReserveRow[]> {
  return db.all<NetReserveRow[]>(`
      SELECT who, 
             SUM(CASE WHEN event_type = 'reserve' THEN amount ELSE 0 END) - 
             SUM(CASE WHEN event_type = 'unreserve' THEN amount ELSE 0 END) AS net_reserve
      FROM events
      GROUP BY who
    `);
}

export function writeToCSV(rows: NetReserveRow[], filePath: string) {
  const writeStream = fs.createWriteStream(filePath);
  writeStream.write("Address,Net Reserve\n");

  rows.forEach((row) => {
    writeStream.write(`${row.who},${row.net_reserve}\n`);
  });

  writeStream.end();
}
