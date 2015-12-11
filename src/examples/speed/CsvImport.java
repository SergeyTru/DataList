package examples.speed;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CsvImport implements AutoCloseable
{
  private final BufferedReader reader;

  public CsvImport(File origin) throws FileNotFoundException
  {
    reader = new BufferedReader(new InputStreamReader(new FileInputStream(origin), StandardCharsets.UTF_8));
  }

  public List<Comparable> readLine() throws IOException
  {
    String line = reader.readLine();
    if (line == null)
      return null;
    ArrayList<Comparable> list = new ArrayList<>();
    int fromPos = 0;
    int tillPos;
    while ((tillPos = line.indexOf('\t', fromPos)) >= 0)
    {
      String text = line.substring(fromPos, tillPos);
      fromPos = tillPos + 1;

      if (text.isEmpty())
        list.add(null);
      else if (text.startsWith("i"))
        list.add(Integer.parseInt(text.substring(1)));
      else if (text.startsWith("l"))
        list.add(Long.parseLong(text.substring(1)));
      else if (text.startsWith("."))
        list.add(Double.parseDouble(text.substring(1)));
      else if (text.startsWith("c"))
        list.add(new Date(Long.parseLong(text.substring(1))));
      else if (text.startsWith(" "))
        list.add(text.substring(1));
      else
        throw new IllegalStateException("Unknown prefix " + text.substring(0, 1));
    }
    return list;
  }

  @Override
  public void close() throws IOException
  {
    reader.close();
  }
}
