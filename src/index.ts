import { ApiPromise, WsProvider } from "@polkadot/api";
import {
  fetchNetReserves,
  getLastProcessedBlock,
  insertEvent,
  openDb,
  setLastProcessedBlock,
  writeToCSV,
} from "./db";
import sqlite3 from "sqlite3";
import { Database } from "sqlite";
import { BN } from "@polkadot/util";
import { Bar, Presets } from "cli-progress";

async function main() {
  // Initialise the provider to connect to the local node
  const provider = new WsProvider("wss://rpc.polkadot.io");

  // Create the API and wait until ready
  const api = await ApiPromise.create({ provider });

  // Retrieve the chain & node information information via rpc calls
  const [chain, nodeName, nodeVersion] = await Promise.all([
    api.rpc.system.chain(),
    api.rpc.system.name(),
    api.rpc.system.version(),
  ]);

  console.log(`Connected to ${chain} using ${nodeName} v${nodeVersion}`);

  // Open db
  const db = await openDb();

  const startBlock = (await getLastProcessedBlock(db)) + 1;
  const endBlockHash = await api.rpc.chain.getFinalizedHead();
  const endBlock = (
    await api.rpc.chain.getHeader(endBlockHash)
  ).number.toNumber();

  const totalBlocks = endBlock - startBlock;
  const progressBar = new Bar(
    {
      format:
        "Progress [{bar}] {percentage}% | ETA: {eta_formatted} | {value}/{total}",
    },
    Presets.shades_classic,
  );

  progressBar.start(totalBlocks, 0);

  let curBlock = startBlock;
  while (curBlock < endBlock) {
    await processBlock(db, api, curBlock);
    progressBar.update(curBlock - startBlock + 1);
    curBlock++;
  }

  console.log("Done scraping!");

  let netReserves = await fetchNetReserves(db);
  await writeToCSV(netReserves, "polkadot.csv");
  console.log(JSON.stringify(netReserves, null, 2));

  console.log("Written to csv");

  // Close db
  await db.close();
}

async function processBlock(
  db: Database<sqlite3.Database, sqlite3.Statement>,
  api: ApiPromise,
  n: number,
) {
  // Get extrinsics from this block
  const blockHash = await api.rpc.chain.getBlockHash(n);
  const { block } = await api.rpc.chain.getBlock(blockHash);
  const { extrinsics } = block;
  const events = (await api.query.system.events.at(blockHash)).toHuman();
  if (!extrinsics) return;
  for (let i = 0; i++; extrinsics.length) {
    const ex = extrinsics[i].toHuman();
    // @ts-ignore
    const { section, method } = ex.method;
    // In some spec version the name is different 😂
    if (section !== "electionsPhragmen" && section != "phragmenElection")
      return;

    // Handle finding a submitCandidacy
    if (method === "submitCandidacy") {
      // @ts-ignore
      const reserveEvents = events.filter(
        (e: any) =>
          e.phase.ApplyExtrinsic === i.toString() &&
          e.event.section === "balances" &&
          e.event.method === "Reserved",
      );
      if (reserveEvents.length !== 1) {
        console.log(
          "Error: Found more or less than one Reserved event for submitCandidacy extrinsic",
        );
        process.exit(1);
      }

      let event = reserveEvents[0].event;
      console.log("Found event!");
      console.log(event);

      // @ts-ignore
      if (event.data.who !== ex.signer.Id) {
        console.error("Event doesn't match signer");
        process.exit(1);
      }

      // @ts-ignore
      const reserveAmount = parseAmountFromEventData(event.data);
      // @ts-ignore
      await insertEvent(db, n, "reserve", reserveAmount, ex.signer.Id);
    }

    // Handle finding a renounceCandidacy
    if (method === "renounceCandidacy") {
      // @ts-ignore
      const unreserveEvents = events.filter(
        (e: any) =>
          e.phase.ApplyExtrinsic === i.toString() &&
          e.event.section === "balances" &&
          e.event.method === "Unreserved",
      );
      if (unreserveEvents.length !== 1) {
        console.error(
          "Found more or less than one Unreserved event for renounceCandidacy extrinsic",
        );
        process.exit(1);
      }

      let event = unreserveEvents[0].event;
      console.log("Found event!");
      console.log(event);

      // @ts-ignore
      if (event.data.who !== ex.signer.Id) {
        console.error("Event doesn't match signer");
        process.exit(1);
      }

      const unreserveAmount = parseAmountFromEventData(event.data);
      // @ts-ignore
      await insertEvent(db, n, "unreserve", unreserveAmount, ex.signer.Id);
    }
  }

  await setLastProcessedBlock(db, n);
}

function parseAmountFromEventData(eventData: any): BN {
  // Somehow event data can be an array or an object...
  let v;
  if (Array.isArray(eventData)) {
    v = eventData[1];
  } else {
    v = eventData.amount;
  }

  // Remove commas
  v = v.replace(/,/g, "");

  // Check if the value ends with ' DOT' and remove the suffix if it does
  const isDot = v.endsWith(" DOT");
  if (isDot) {
    v = v.substring(0, v.length - 4);
  }

  // Convert the string to a BigNumber
  let value = new BN(v);

  // If the value was in DOT, convert it to Planck (multiply by 10^10)
  if (isDot) {
    value = value.mul(new BN(10).pow(new BN(10)));
  }

  return value;
}

main();
