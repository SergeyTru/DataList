package datalist.list;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * <tt>Database</tt> class encapsulates directory where <tt>DataLists</tt> stores and generates file names for lists,
 * tables, indexes, text search indexes and other.
 * @author SergeyT
 */
public class Database
{
  private final File dbDirectory;

  public Database(File dbDirectory)
  {
    this.dbDirectory = dbDirectory;
    this.dbDirectory.mkdirs();
  }

  public static Database temporary() throws IOException
  {
    return new Database(Files.createTempDirectory("datalist_db_").toFile());
  }

  public File getDirectory()
  {
    return dbDirectory;
  }

  /** Backup whole DB directory to one archive and return this archive file name */
  public File backup() throws IOException
  {
    byte[] buffer = new byte[2048];
    File zipFile = new File(dbDirectory.getParentFile(), dbDirectory.getName() + ".backup");
    try (ZipOutputStream outputZip = new ZipOutputStream(new FileOutputStream(zipFile)))
    {
      outputZip.setLevel(Deflater.BEST_SPEED);
      for (File file: dbDirectory.listFiles())
      {
        ZipEntry ze = new ZipEntry(file.getName()); //works correctly till no subdirs
        ze.setTime(file.lastModified());
        outputZip.putNextEntry(ze);
        try (FileInputStream in = new FileInputStream(file))
        {
          int len;
          while ((len = in.read(buffer)) >= 0)
            outputZip.write(buffer, 0, len);
        }
        outputZip.closeEntry();
      }
    }
    return zipFile;
  }

  public File getDataFile(String tableName)
  {
    return new File(dbDirectory, tableName + "-data");
  }

  public File getItemOffsetsFile(String tableName)
  {
    return new File(dbDirectory, tableName + "-index");
  }

  public File getCrossIndexFile(String fromTable, String toTable)
  {
    return new File(dbDirectory, fromTable + "2" + toTable + ".idx");
  }

  public File getTextSearchFile(String table)
  {
    return new File(dbDirectory, table + ".ts");
  }

  public File getTextSearchIndexFile(String table)
  {
    return new File(dbDirectory, table + "-index.ts");
  }
}
