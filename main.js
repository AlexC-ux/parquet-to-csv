import sqlite3 from "sqlite3";
import pl from "nodejs-polars";
import fg from "fast-glob";
import fs from "fs";
import csv from "fast-csv";
import path from "path";
// Получает все файлы виды *.parquet из текущей папки проекта
const files = fg.globSync("**/*.parquet");
console.log(`found files count: ${files.length}`);

const maxDurationMs = 2650;
const allKeys = new Set();

async function getRecords() {
  const dedupedRecords = new Set();

  async function readParquetToCsv(parquetFilePath) {
    try {
      console.log(`reading ${parquetFilePath}`);
      const df = await pl.scanParquet(parquetFilePath).collect();
      console.log(`converting file ${parquetFilePath} to records array`);
      const parquetRecords = df.toRecords();
      console.log(`records deduplication`);
      for (const record of parquetRecords) {
        for (const key of Object.keys(record)) {
          allKeys.add(key);
        }
        // Отбор уникальных записей
        const dem = 1000000n;
        const durationMs = Number(
          BigInt(record.end_time) / dem - BigInt(record.start_time) / dem
        );

        dedupedRecords.add({
          duration_ms: durationMs,
          ...record,
        });
      }
    } catch (error) {
      console.error(error);
    }
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

  const promises = [];
  for (const filePath of files) {
    promises.push(readParquetToCsv(filePath));
  }

  await Promise.all(promises);

  const resultRecords = [];

  for (const record of dedupedRecords) {
    resultRecords.push(addNotFoundKeys(record));
  }

  return resultRecords;
}

const recordsToSave = await getRecords();
const endTimestamp = Date.now();

/**
 *
 * @param {boolean} makeSqlite should create sqlite output
 * @param {boolean} makeCsv should create csv output
 */
function writeRecords(makeSqlite, makeCsv) {
  if (!fs.existsSync("output")) {
    fs.mkdirSync("output");
  }
  if (makeCsv) {
    console.log("writing result file");
    const ws = fs.createWriteStream(
      path.join(process.cwd(), "output", `output-${endTimestamp}.csv`)
    );

    const wsLong = fs.createWriteStream(
      path.join(process.cwd(), "output", `output-${endTimestamp}.csv`)
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
  if (makeSqlite) {
    const sqliteOutputPath = path.join(
      process.cwd(),
      "output",
      `output-${endTimestamp}.sqlite`
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
          "end_time",
          "start_time",
          "",
        ];
        // Create a table
        const createCommand = `CREATE TABLE trace (
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
              stmt.run(...insertValues, function (err) {
                if (err) {
                  console.error("Error inserting record:", err);
                  console.log(statementText);
                  console.log({ insertValues });
                } else {
                  console.log(
                    `A row has been inserted with rowid ${this.lastID}`
                  );
                }
              });
            }
            console.log("closing db");
            db.close((err) => {
              if (err) {
                console.error("Error closing database:", err.message);
              } else {
                console.log("Database connection closed.");
              }
            });
          }
        });
      }
    });
  }
}

writeRecords(true, false);
