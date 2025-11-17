import fs from "fs";

export async function deleteEmptyFilesInDirectory(directoryPath) {
  try {
    const files = await fs.readdir(directoryPath);

    for (const file of files) {
      const filePath = path.join(directoryPath, file);
      const stats = await fs.stat(filePath);

      // Check if it's a file and its size is 0 bytes
      if (stats.isFile() && stats.size === 0) {
        await fs.unlink(filePath);
        console.log(`Deleted empty file: ${filePath}`);
      }
    }
    console.log("Finished checking for empty files.");
  } catch (error) {
    console.error(`Error deleting empty files: ${error.message}`);
  }
}
