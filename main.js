import sqlite3 from "sqlite3";
import pl from "nodejs-polars";
import fg from "fast-glob";
import fs from "fs";
import csv from "fast-csv";
import path from "path";
import crypto from "crypto";
import ProgressBar from "progress";
import { v4 as uuidv4 } from "uuid";
import * as dotenv from "dotenv";
dotenv.config();

const writecsv = !!parseInt(process.env.WRITE_CSV ?? "0");
const writesqlite = !!parseInt(process.env.WRITE_SQLITE ?? "0");

if (!writecsv && !writesqlite) {
  throw "Не выбрано ни одного типа данных для вывода, проверьте readme и .env";
}

console.log(`${writecsv ? "✅" : "❌"} Вывод в csv `);
console.log(`${writesqlite ? "✅" : "❌"} Вывод в sqlite`);

const cacheFolder = path.join(process.cwd(), ".cache");
if (!fs.existsSync(cacheFolder)) {
  fs.mkdirSync(cacheFolder, { recursive: true });
}
// Получает все файлы виды *.parquet из текущей папки проекта
const files = fg.globSync("input/**/*.parquet");
console.log(`found input files count: ${files.length}`);

const runTimestamp = Date.now();
const resultDir = path.join(process.cwd(), "output", `${runTimestamp}`);
if (!fs.existsSync(resultDir)) {
  fs.mkdirSync(resultDir, { recursive: true });
}

const maxDurationMs = 2650;
const minDurationMs = 2000;
const allKeys = new Set();

const recordsToWrite = [];

async function generateSHA256Hash(dataToHash) {
  // Create a SHA-256 hash object
  const hash = crypto.createHash("sha256");

  // Update the hash object with the data
  hash.update(dataToHash);

  // Get the digest (the resulting hash) in hexadecimal format
  const sha256Hash = hash.digest("hex");
  return sha256Hash;
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunkArray(myArray, chunkSize) {
  const results = [];
  let index = 0;

  while (index < myArray.length) {
    results.push(myArray.slice(index, index + chunkSize));
    index += chunkSize;
  }

  return results;
}

async function getRecords() {
  const convertingBar = new ProgressBar(
    "Converting parquet files [:bar] :percent eta: :etas\n",
    {
      total: files.length * 2,
      width: 30,
      complete: "█",
      incomplete: "▒",
    }
  );

  async function readParquetToCsv(parquetFilePath) {
    const dedupedRecords = new Set();
    try {
      convertingBar.tick();
      const pathHash = path.parse(parquetFilePath).name;
      const cacheGlob = `${pathHash}.*.json`;
      const cacheChunks = fg.globSync(cacheGlob, {
        cwd: cacheFolder,
      });
      const cacheExists = cacheChunks.length > 0;
      let parquetRecords;
      if (cacheExists) {
        let resultRecords = [];
        for (const chunkFile of cacheChunks) {
          const chunkContent = JSON.parse(
            fs.readFileSync(path.join(cacheFolder, chunkFile))
          );
          resultRecords.push(...chunkContent);
        }
        parquetRecords = resultRecords;
      } else {
        try {
          const df = await pl.scanParquet(parquetFilePath).collect();
          parquetRecords = df.toRecords();
          const chunks = chunkArray(parquetRecords ?? [], 10000);
          let chunkIndex = 0;
          for (const chunk of chunks) {
            const cacheChunkFileName = path.join(
              cacheFolder,
              `${pathHash}.${chunkIndex++}.json`
            );
            fs.writeFileSync(
              cacheChunkFileName,
              JSON.stringify(chunk, (_, value) =>
                typeof value === "bigint" ? value.toString() : value
              )
            );
          }
        } catch (error) {
          console.warn("Parquet data extraction error handled!");
          console.warn(`Errored file: ${parquetFilePath}`);
          console.error(error);
          return [];
        }
      }
      const recordsProcessingBar = new ProgressBar(
        "Processing records deduplication [:bar] :percent eta: :etas\n",
        {
          total: parquetRecords.length * 2,
          width: 30,
          complete: "█",
          incomplete: "▒",
          renderThrottle: 300,
        }
      );
      for (const recordInfo of parquetRecords) {
        recordsProcessingBar.tick();
        const dem = 1000000n;
        const durationMs = Number(
          BigInt(recordInfo.end_time) / dem -
            BigInt(recordInfo.start_time) / dem
        );

        if (minDurationMs > durationMs) {
          recordsProcessingBar.tick();
          continue;
        }

        const record = {
          duration_ms: durationMs,
          ...recordInfo,
        };
        for (const key of Object.keys(record)) {
          allKeys.add(key);
        }

        dedupedRecords.add(record);
        recordsProcessingBar.tick();
      }
    } catch (error) {
      console.error(error);
    }
    convertingBar.tick();
    return dedupedRecords;
  }

  function addNotFoundKeys(obj) {
    const objKeys = Object.keys(obj);
    if (objKeys.length == allKeys.length) {
      return obj;
    } else {
      const notFoundKeys = [];
      for (const key of allKeys) {
        if (!objKeys.includes(key)) {
          notFoundKeys.push(key);
        }
      }
      const newObj = { ...obj };
      for (const notFoundKey of notFoundKeys) {
        newObj[notFoundKey] = null;
      }
      return newObj;
    }
  }

  for (const filePath of files) {
    const resultRecords = [];
    const records = await readParquetToCsv(filePath);
    for (const record of records) {
      resultRecords.push(addNotFoundKeys(record));
    }
    recordsToWrite.push(resultRecords);
  }

  for (const records of recordsToWrite) {
    await writeRecords(records, writesqlite, writecsv);
  }
}

/**
 *
 * @param {boolean} writeSqlite should create sqlite output
 * @param {boolean} writeCsv should create csv output
 */
async function writeRecords(recordsToSave, writeSqlite, writeCsv) {
  if (!fs.existsSync("output")) {
    fs.mkdirSync("output");
  }
  if (writeCsv) {
    console.log("writing result file");
    const ws = fs.createWriteStream(
      path.join(resultDir, `output-${runTimestamp}.${writeTimestamp}.$.csv`)
    );

    const wsLong = fs.createWriteStream(
      path.join(resultDir, `output-${runTimestamp}.${writeTimestamp}.csv`)
    );

    csv
      .write(recordsToSave, {
        headers: true,
      })
      .pipe(ws)
      .on("finish", () => {
        console.log("CSV main file written successfully!");
      });

    console.log("writing long queries");
    csv
      .write(
        recordsToSave.filter((d) => d.duration_ms >= maxDurationMs),
        {
          headers: true,
        }
      )
      .pipe(wsLong)
      .on("finish", () => {
        console.log("CSV long file written successfully!");
      });
  }
  if (writeSqlite) {
    const insertSqliteBar = new ProgressBar(
      "Sqlite inserting records [:bar] :percent eta: :etas\n",
      {
        total: recordsToSave.length + 2,
        width: 30,
        complete: "█",
        incomplete: "▒",
        renderThrottle: 1000,
      }
    );
    insertSqliteBar.tick();
    const sqliteOutputPath = path.join(
      resultDir,
      `output-${runTimestamp}.sqlite`
    );
    // Open a database connection (creates the file if it doesn't exist)
    /**
     * @type {sqlite3.Database}
     */
    const db = await new Promise((res, rej) => {
      new sqlite3.Database(sqliteOutputPath, function (err) {
        if (err) {
          rej(err);
        } else {
          res(this);
        }
      });
    });

    const numberKeys = [
      "_timestamp",
      "duration",
      "duration_ms",
      "end_time",
      "start_time",
      "",
    ];
    // Create a table
    const createCommand = `CREATE TABLE IF NOT EXISTS trace (
UID TEXT PRIMARY KEY,
${[...allKeys]
  .map((key) => `${key} ${numberKeys.includes(key) ? "INTEGER" : "TEXT"}`)
  .join(",\r\n")}
    )`;
    console.log({ createCommand });
    await new Promise((res, rej) => {
      db.run(createCommand, function (err) {
        if (err) {
          rej(err);
        } else {
          res(true);
        }
      });
    });
    let activeRequests = 0;
    console.log('Table "trace" created');
    console.log(`inserting ${recordsToSave.length} records started`);
    for (const record of recordsToSave) {
      const statementText = `INSERT INTO trace (uid, ${[...allKeys].join(", ")}) VALUES (?, ${[...allKeys].map((v) => "?").join(", ")})`;
      const stmt = db.prepare(statementText);
      const values = [...allKeys].map((key) => {
        const value = record[key];
        if (!value) {
          return "NULL";
        }
        if (numberKeys.includes(key)) {
          return Number(value);
        } else {
          return `${value}`;
        }
      });
      const insertValues = [uuidv4(), ...values];
      activeRequests++;
      await new Promise((res, rej) => {
        stmt.run(...insertValues, function (err) {
          if (err) {
            rej(err);
          } else {
            res(true);
          }
        });
      });
      await new Promise((res, rej) => {
        stmt.finalize(function (err) {
          if (err) {
            rej(err);
          } else {
            res(true);
          }
        });
      });
    }

    console.log("trying to close db connection");
    await new Promise((res, rej) => {
      db.close(function (err) {
        if (err) {
          console.error("Error closing database:", err.message);
          rej(err);
        } else {
          res(true);
          console.log("Database connection closed.");
        }
      });
    });
  }
}

await getRecords();
