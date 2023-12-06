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
  console.log("Opened db");

  let netReservesSoFar = await fetchNetReserves(db);
  console.log("Net reserves so far:");
  console.log(JSON.stringify(netReservesSoFar, null, 2));

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

  console.log(
    `Scraping blocks ${startBlock} to ${endBlock} (${totalBlocks} total)`,
  );

  progressBar.start(totalBlocks, 0);

  let curBlock = startBlock;
  while (curBlock < endBlock) {
    const success = await processBlock(db, api, curBlock);
    if (success) {
      progressBar.update(curBlock - startBlock + 1);
      curBlock++;
    } else {
      console.log("Retrying in 60 seconds");
      await new Promise((resolve) => setTimeout(resolve, 60_000));
    }
  }

  console.log("Done scraping!");

  let netReserves = await fetchNetReserves(db);
  writeToCSV(netReserves, "polkadot.csv");
  console.log(JSON.stringify(netReserves, null, 2));

  console.log("Written to csv");

  // Close db
  await db.close();
}

// returns whether successful
async function processBlock(
  db: Database<sqlite3.Database, sqlite3.Statement>,
  api: ApiPromise,
  n: number,
): Promise<boolean> {
  await db.run("BEGIN TRANSACTION");
  try {
    // Get extrinsics from this block
    const blockHash = await api.rpc.chain.getBlockHash(n);
    const { block } = await api.rpc.chain.getBlock(blockHash);
    const { extrinsics } = block;
    const events = (await api.query.system.events.at(blockHash)).toHuman();
    for (let i = 0; i < extrinsics.length; i++) {
      const ex = extrinsics[i].toHuman();
      // @ts-ignore
      const { section, method } = ex.method;

      // In some spec version the name is different 😂
      if (section !== "electionsPhragmen" && section != "phragmenElection")
        continue;

      // Handle finding a submitCandidacy
      if (method === "submitCandidacy") {
        // @ts-ignore
        const reserveEvents = events.filter(
          (e: any) =>
            e.phase.ApplyExtrinsic === i.toString() &&
            e.event.section === "balances" &&
            e.event.method === "Reserved",
        );
        if (reserveEvents.length === 0) {
          console.warn(
            "Skipping because it appears it failed, no expected event was emitted with the extrinsic.",
          );
          console.warn({ n });
          continue;
        }
        if (reserveEvents.length > 1) {
          console.log(
            "Error: Found more than one Reserved event for submitCandidacy extrinsic",
          );
          console.error({ reserveEvents, n });
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

        const { amount, who } = parseAmountAndWhoFromEventData(event.data);
        await insertEvent(db, n, "reserve", amount, who);
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
        if (unreserveEvents.length === 0) {
          console.warn(
            "Skipping because it appears it failed, no expected event was emitted with the extrinsic.",
          );
          console.warn({ n });
          continue;
        }
        if (unreserveEvents.length > 1) {
          console.error(
            "Found more than one Unreserved event for renounceCandidacy extrinsic",
          );
          console.error({ unreserveEvents, n });
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

        const { amount, who } = parseAmountAndWhoFromEventData(event.data);
        await insertEvent(db, n, "unreserve", amount, who);
      }
    }
    await setLastProcessedBlock(db, n);
    await db.run("COMMIT");
    return true;
  } catch (error) {
    console.error("Error processing block", error);
    await db.run("ROLLBACK");
    return false;
  }
}

function parseAmountAndWhoFromEventData(eventData: any): {
  amount: string;
  who: string;
} {
  // Somehow event data can be an array or an object...
  let amount;
  let who;
  if (Array.isArray(eventData)) {
    who = eventData[0];
    amount = eventData[1];
  } else {
    who = eventData.who;
    amount = eventData.amount;
  }

  // Remove commas
  amount = amount.replace(/,/g, "");

  // Check if the value ends with ' DOT' and remove the suffix if it does
  const isDot = amount.endsWith(" DOT");
  if (isDot) {
    amount = amount.substring(0, amount.length - 4);
  }

  // Convert the string to a BigNumber
  amount = new BN(parseInt(amount));

  // If the value was in DOT, convert it to Planck (multiply by 10^10)
  if (isDot) {
    amount = amount.mul(new BN(10).pow(new BN(10)));
  }

  return {
    who,
    amount: amount.toString(),
  };
}

main();
