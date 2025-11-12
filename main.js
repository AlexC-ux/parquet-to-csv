import pl from "nodejs-polars";
import fg from "fast-glob";
import fs from "fs";
import csv from "fast-csv";
import path from "path";
// Получает все файлы виды *.parquet из текущей папки проекта
const files = fg.globSync("**/*.parquet");
console.log(`found files count: ${files.length}`);

const dedupedRecords = new Set();

async function readParquetToCsv(parquetFilePath) {
  try {
    console.log(`reading ${parquetFilePath}`);
    const df = await pl.scanParquet(parquetFilePath).collect();
    console.log(`converting file ${parquetFilePath} to records array`);
    const parquetRecords = df.toRecords();
    console.log(`records deduplication`);
    for (const record of parquetRecords) {
      // Отбор уникальных записей
      dedupedRecords.add(record);
    }
  } catch (error) {
    console.error("Error reading Parquet from S3:", error);
  }
}

const promises = [];
for (const filePath of files) {
  promises.push(readParquetToCsv(filePath));
}

await Promise.all(promises);
console.log("writing result file");
const ws = fs.createWriteStream(
  path.join(process.cwd(), 'output',`output-${Date.now()}.csv`)
);

csv
  .write([...dedupedRecords], { headers: true })
  .pipe(ws)
  .on("finish", () => {
    console.log("CSV file written successfully!");
  });
