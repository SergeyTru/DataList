package datalist.table;

import datalist.list.DataList;
import datalist.list.Database;
import static datalist.stream.DBDataStreamers.strings;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TextSearchTest
{
  @Test
  public void testSimple() throws IOException
  {
    File dbDir = Files.createTempDirectory("test-text-search-").toFile();
    Database db = new Database(dbDir);
    try
    {
      DataList<MyTableRow> table = new DataList<>(db, "test", new TableRowStreamer<MyTableRow>(strings, strings) {
        @Override
        protected MyTableRow createObject(Comparable[] data)
        {
          return new MyTableRow(data);
        }
      });
      TextSearch ts = new TextSearch(table);
      table.addAll(Arrays.asList(
          new MyTableRow("Sergey Andreevich", "Andrey Antonovich"),
          new MyTableRow("Dmitry Andreevich", "Andrey Dmitrievich")));

      TextSearchResult sr = ts.findText("andr", null);
      assertEquals(new SortedIntSet(0, 1), sr.getColumns());
      assertEquals(new SortedIntSet(0, 1), sr.getRows());

      sr = ts.findText("ser", null);
      assertEquals(new SortedIntSet(new int[]{0}), sr.getColumns());
      assertEquals(new SortedIntSet(new int[]{0}), sr.getRows());

      sr = ts.findText("Anton", null); //prefix 'an', skip first
      assertEquals(new SortedIntSet(new int[]{1}), sr.getColumns());
      assertEquals(new SortedIntSet(new int[]{0}), sr.getRows());
    }
    finally
    {
      for (File file: dbDir.listFiles())
        if (!file.delete())
          file.deleteOnExit();
      if (!dbDir.delete())
        dbDir.deleteOnExit();
    }
  }
  
  private static class MyTableRow extends SimpleTableRow
  {
    public MyTableRow(Comparable... texts)
    {
      super(texts, texts.length, false);
    }
  }
}
