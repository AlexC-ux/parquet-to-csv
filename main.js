import sqlite3 from "sqlite3";
import pl from "nodejs-polars";
import fg from "fast-glob";
import fs from "fs";
import csv from "fast-csv";
import path from "path";
import crypto from "crypto";
import ProgressBar from "progress";

const writecsv = false;
const writesqlite = true;

const cacheFolder = path.join(process.cwd(), ".cache");
if (!fs.existsSync(cacheFolder)) {
  fs.mkdirSync(cacheFolder, { recursive: true });
}
// Получает все файлы виды *.parquet из текущей папки проекта
const files = fg.globSync("input/**/*.parquet");
console.log(`found files count: ${files.length}`);

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
    "Converting parquet files [:bar] :percent :etas\n",
    {
      total: files.length * 2,
      width: 30,
      complete: "█",
      incomplete: "▒",
    }
  );

  async function readParquetToCsv(parquetFilePath, fileIndex) {
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
        const df = await pl.scanParquet(parquetFilePath).collect();
        parquetRecords = df.toRecords();
        const chunks = chunkArray(parquetRecords, 10000);
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

  let fileIndex = 0;

  for (const filePath of files) {
    const resultRecords = [];
    const records = await readParquetToCsv(filePath, fileIndex++);
    for (const record of records) {
      resultRecords.push(addNotFoundKeys(record));
    }
    recordsToWrite.push(resultRecords);
  }

  for (const records of recordsToWrite) {
    writeRecords(records, writesqlite, writecsv);
  }
}

/**
 *
 * @param {boolean} writeSqlite should create sqlite output
 * @param {boolean} writeCsv should create csv output
 */
function writeRecords(recordsToSave, writeSqlite, writeCsv) {
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
    const additionalFields = [...allKeys].filter((d) => d != "span_id");
    // Open a database connection (creates the file if it doesn't exist)
    const db = new sqlite3.Database(sqliteOutputPath, (err) => {
      if (err) {
        console.error("Error opening database:", err.message);
      } else {
        console.log("Connected to the SQLite database.");

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
span_id TEXT PRIMARY KEY,
${additionalFields
  .map((key) => `${key} ${numberKeys.includes(key) ? "INTEGER" : "TEXT"}`)
  .join(",\r\n")}
    )`;
        console.log({ createCommand });
        db.run(createCommand, (err) => {
          if (err) {
            console.error("Error creating table:", err.message);
          } else {
            console.log('Table "trace" created');
            const insertPromises = [];
            console.log(`inserting ${recordsToSave.length} records started`);
            for (const record of recordsToSave) {
              const statementText = `INSERT INTO trace (span_id, ${additionalFields.join(", ")}) VALUES (?, ${additionalFields.map((v) => "?").join(", ")})`;
              const stmt = db.prepare(statementText);
              const values = additionalFields.map((key) => {
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
              const insertValues = [record.span_id, ...values];
              // Execute the INSERT statement with values
              const promise = new Promise((res, rej) => {
                stmt.run(...insertValues, function (err) {
                  if (err) {
                    insertSqliteBar.tick();
                    console.error("Error inserting record:", err);
                    console.log(statementText);
                    console.log({ insertValues });
                    rej(err);
                  } else {
                    insertSqliteBar.tick();
                    res(true);
                  }
                });
              });
              insertPromises.push(promise);
            }
            Promise.allSettled(insertPromises)
              .then(() => {
                console.log("inserts ok!");
              })
              .finally(() => {
                db.close((err) => {
                  if (err) {
                    console.error("Error closing database:", err.message);
                  } else {
                    console.log("Database connection closed.");
                  }
                });
              });
          }
        });
      }
    });
  }
}

await getRecords();
