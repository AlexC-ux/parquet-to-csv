import fs from "fs";
import path from "path";

export function deleteEmptyFilesInDirectory(directoryPath) {
  try {
    const files = fs.readdirSync(directoryPath);

    for (const file of files) {
      const filePath = path.join(directoryPath, file);
      const stats = fs.statSync(filePath);

      // Check if it's a file and its size is 0 bytes
      if (stats.isFile() && stats.size === 0) {
        fs.unlinkSync(filePath);
        console.log(`Deleted empty file: ${filePath}`);
      }
    }
    console.log("Finished checking for empty files.");
  } catch (error) {
    console.error(`Error deleting empty files: ${error.message}`);
  }
}
