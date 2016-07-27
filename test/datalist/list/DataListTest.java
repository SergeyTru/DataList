package datalist.list;

import datalist.index.SortedIndex;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.stream.DBDataStreamer;
import datalist.stream.DBDataStreamers;
import datalist.types.SortedIntSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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

  @Test
  public void testComplexIndex() throws IOException
  {
    DBDataStreamer<Set<Integer>> intSetStreamer = new DBDataStreamer<Set<Integer>>()
    {
      @Override
      public Set<Integer> read(ReadBuffer buffer) throws DatabaseBrokenError
      {
        int cnt = buffer.getInt();
        Set<Integer> result = new HashSet<>(cnt);
        while (--cnt >= 0)
          result.add(buffer.getInt());
        return result;
      }

      @Override
      public void write(WriteBuffer buffer, Set<Integer> item)
      {
        buffer.putInt(item.size());
        item.forEach(buffer::putInt);
      }
    };
    Database db = Database.temporary();
    try (DataList<Set<Integer>> intsDB = new DataList<>(db, "test", intSetStreamer))
    {
      SortedIndex<Integer> index = new SortedIndex<>(Files.createTempFile("test-", "").toFile(), DBDataStreamers.ints_no_nulls);
      intsDB.withMulticastIndex(index, x->x, true);
      LinkedHashSet<Integer> set1 = new LinkedHashSet<>(Arrays.asList(100, 200));
      LinkedHashSet<Integer> set2 = new LinkedHashSet<>(Arrays.asList(200, 300));
      LinkedHashSet<Integer> set3 = new LinkedHashSet<>(70);
      set3.add(200);
      set3.add(100);
      LinkedHashSet<Integer> set4 = new LinkedHashSet<>(Arrays.asList(2));
      List<Set<Integer>> dataList = Arrays.asList(set1, set2, set3, set4);
      intsDB.addAll(dataList);
      assertEquals(0, intsDB.indexOf(set1));
      assertEquals(2, intsDB.lastIndexOf(set1));
      assertEquals(1, intsDB.indexOf(set2));
      assertEquals(1, intsDB.lastIndexOf(set2));
      assertEquals(0, intsDB.indexOf(set3));
      assertEquals(2, intsDB.lastIndexOf(set3));
    }
  }
}