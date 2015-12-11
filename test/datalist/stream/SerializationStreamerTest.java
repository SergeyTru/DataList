package datalist.stream;

import datalist.list.DataList;
import datalist.list.Database;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class SerializationStreamerTest
{
  @Test
  public void testSerialization() throws IOException
  {
    List<String> countries1 = Arrays.asList("Qatar", "Luxembourg", "Liechtenstein");
    List<String> countries2 = Arrays.asList("Norway", "San Marino", "Sweden");

    Database db = Database.temporary();
    try (DataList<List<String>> list = new DataList<>(db, "test", new SerializationStreamer<List<String>>()))
    {
      list.addAll(Arrays.asList(countries1, countries2));
      assertEquals(countries1, list.get(0));
      assertEquals(countries2, list.get(1));
      assertEquals(countries1, list.get(0));
      assertEquals(countries2, list.get(1));
    }
  }
}
