package datalist.list;

import datalist.index.SortedIndex;
import datalist.stream.DBDataStreamers;
import datalist.types.SortedIntSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

public class DataListTest
{
  @Test
  public void testSimple() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_with_nulls))
    {
      longDB.addAll(Arrays.<Long>asList(100L, 200L, 300L, 400L));
      Iterator<Long> it = longDB.iterator();
      assertEquals(Long.valueOf(100), it.next());
      assertEquals(Long.valueOf(200), it.next());
      assertEquals(Long.valueOf(300), it.next());
      assertEquals(Long.valueOf(400), it.next());
      assertFalse(it.hasNext());
    }
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_with_nulls))
    {
      Iterator<Long> it = longDB.iterator();
      assertEquals(Long.valueOf(100), it.next());
      assertEquals(Long.valueOf(200), it.next());
      assertEquals(Long.valueOf(300), it.next());
      assertEquals(Long.valueOf(400), it.next());
      assertFalse(it.hasNext());
    }
  }

  @Test
  public void testAddAndGet() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_with_nulls))
    {
      longDB.addAll(Arrays.<Long>asList(100L, 200L, 300L, 400L));
      Iterator<Long> it = longDB.iterator();
      assertEquals(Long.valueOf(100), it.next());
      assertEquals(Long.valueOf(200), it.next());
      assertEquals(Long.valueOf(300), it.next());
      assertEquals(Long.valueOf(400), it.next());
      assertFalse(it.hasNext());
      longDB.addAll(Arrays.<Long>asList(500L, 700L));
      assertEquals(Long.valueOf(700), longDB.get(longDB.size()-1));
      assertEquals(Long.valueOf(500), longDB.get(longDB.size()-2));
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testClearAndGet() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_with_nulls))
    {
      longDB.addAll(Arrays.<Long>asList(100L, 200L, 300L, 400L));
      longDB.clear();
      assertEquals(100L, (long)longDB.get(0));
    }
  }

  @Test
  public void testByOne() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_with_nulls))
    {
      longDB.add(100L);
      assertEquals(Long.valueOf(100), longDB.get(0));
      longDB.add(200L);
      assertEquals(Long.valueOf(200), longDB.get(1));
    }
  }

  @Test
  public void testIndexOfNoIndex() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_no_nulls))
    {
      longDB.addAll(Arrays.<Long>asList(100L, 200L, 300L, 400L, 300L, 200L, 100L));
      assertEquals(1, longDB.indexOf(200L));
      assertEquals(5, longDB.lastIndexOf(200L));
      assertEquals(0, longDB.indexOf(100L));
      assertEquals(6, longDB.lastIndexOf(100L));
    }
  }

  @Test
  public void testIndexOfWithIndex() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_no_nulls))
    {
      longDB.withIndex(new SortedIndex<>(Files.createTempFile("test-", "").toFile(), DBDataStreamers.longs_no_nulls), x->x%200, true);
      longDB.addAll(Arrays.<Long>asList(100L, 200L, 300L, 400L, 300L, 200L, 100L));
      assertEquals(1, longDB.indexOf(200L));
      assertEquals(5, longDB.lastIndexOf(200L));
      assertEquals(0, longDB.indexOf(100L));
      assertEquals(6, longDB.lastIndexOf(100L));
      assertEquals(3, longDB.indexOf(400L));
      assertEquals(3, longDB.lastIndexOf(400L));
    }
  }

  @Test
  public void testSublist() throws IOException
  {
    Database db = Database.temporary();
    try (DataList<Long> longDB = new DataList<>(db, "test", DBDataStreamers.longs_no_nulls))
    {
      longDB.withIndex(new SortedIndex<>(Files.createTempFile("test-", "").toFile(), DBDataStreamers.longs_no_nulls), x->x%200, true);
      longDB.addAll(Arrays.<Long>asList(100L, 200L, 300L, 400L, 300L, 200L, 100L));
      Iterator<Long> it = longDB.sublist(new SortedIntSet(1, 3, 6)).iterator();
      assertEquals(Long.valueOf(200), it.next());
      assertEquals(Long.valueOf(400), it.next());
      assertEquals(Long.valueOf(100), it.next());
      assertFalse(it.hasNext());
    }
  }
}