import pl from "nodejs-polars";
import fg from "fast-glob";
import fs from "fs";
import csv from "fast-csv";
import path from "path";
// Получает все файлы виды *.parquet из текущей папки проекта
const files = fg.globSync("**/*.parquet");
console.log(`found files count: ${files.length}`);

const maxDurationMs = 2650;

async function getRecords() {
  const dedupedRecords = new Set();
  const allKeys = new Set();

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

console.log("writing result file");
if (!fs.existsSync("output")) {
  fs.mkdirSync("output");
}
const ws = fs.createWriteStream(
  path.join(process.cwd(), "output", `output-${Date.now()}.csv`)
);

const wsLong = fs.createWriteStream(
  path.join(process.cwd(), "output", `output-${Date.now()}.csv`)
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
