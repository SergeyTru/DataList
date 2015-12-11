package datalist.index;

import datalist.io.ChannelBuilder;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;
import datalist.stream.FixedSizeDataStreamer;
import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Keep sorted list of values and maps it to indexes.<br>
 * Binary search used to find all rows for range of values.<br>
 * Usable for big amount of data.<br><br>
 *
 * Implementation details: this index works only with fixed size values.<br><br>
 *
 * File structure:<br>
 * 8 bytes for count<br>
 * count*sizeof(value) for keys<br>
 * count*sizeof(int) for keys<br><br>
 * Value with 0-based index K might be readed at pos K*sizeof(value)+8<br>
 * Key with 0-based index K might be readed at pos K*sizeof(int)+count*sizeof(value)+8<br>
 */
public class SortedIndex<T extends Comparable<T>> implements Index<T>, AutoCloseable
{
  private static final int buffSizeR = Integer.getInteger("sortindex_read_buffer_size", -1);
  private static final int buffSizeW = Integer.getInteger("sortindex_write_buffer_size", -1);

  private final FileChannel fc;
  private final FixedSizeDataStreamer<T> keysHandler;
  private long size;

  private volatile T minVal;
  private volatile T maxVal;

  public SortedIndex(File storage, FixedSizeDataStreamer<T> keysHandler) throws IOException
  {
    fc = ChannelBuilder.forReadWrite(storage).build();
    this.keysHandler = keysHandler;
    if (fc.size() > 8)
    {
      try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
      {
        size = rdr.getLong();
        loadMinMax(rdr);
      }
    }
    else
      size = 0;
  }

  private void loadMinMax(ReadBuffer rdr) throws DatabaseBrokenError
  {
    if (size > 0)
    {
      rdr.position(8);
      minVal = keysHandler.read(rdr);
      //we store nulls at the end, skip it
      long nullsFrom = findLowIndex(rdr, null, 0, size);
      rdr.position(8 + (nullsFrom-1) * keysHandler.itemSize());
      maxVal = keysHandler.read(rdr);
    }
    else
    {
      minVal = null;
      maxVal = null;
    }
  }

  @Override
  public void recreate(List<KeyToIndex<T>> values)
  {
    values.sort(KeyToIndex.keysComparator());
    try (WriteBuffer writer = new WriteBuffer(fc, buffSizeW))
    {
      size = values.size();
      writer.position(0);
      writer.putLong(size);
      for (KeyToIndex<T> pair: values)
        keysHandler.write(writer, pair.getKey());

      if (writer.position() != 8 + size*keysHandler.itemSize())
        throw new DatabaseBrokenError("Streamer " + keysHandler.getClass() + " violates general contract!");
      for (KeyToIndex<T> pair: values)
        writer.putInt(pair.getIndex());
    }
    try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
    {
      loadMinMax(rdr);
    }
  }

  @Override
  public void clear()
  {
    try
    {
      size = 0;
      fc.truncate(0);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  public boolean isEmpty()
  {
    return size == 0;
  }

  public T min()
  {
    return minVal;
  }

  public T max()
  {
    return maxVal;
  }

  private final Comparator<T> comparator = Comparator.nullsLast(Comparator.naturalOrder());

  @Override
  public SortedIntSet valuesFor(T min, T max)
  {
    if (min == null || max == null)
      throw new IllegalArgumentException("Range with nulls not allowed");

    if (comparator.compare(min, max) > 0)
      throw new IllegalArgumentException("Inverse range");

    try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
    {
      return doValuesFor(min, max, rdr);
    }
  }

  private SortedIntSet doValuesFor(T min, T max, ReadBuffer rdr)
  {
    if (isEmpty())
      return SortedIntSet.empty();

    if (min == null || max == null)
    {
      if (min != null || max != null)
        throw new IllegalStateException();

      long nullsFrom = findLowIndex(rdr, null, 0, size);
      if (nullsFrom >= size)
        return SortedIntSet.empty();

      rdr.position(nullsFrom * 4 + size * keysHandler.itemSize() + 8);
      SortedIntSet res = new SortedIntSet((int)(size - nullsFrom));
      long remaining = size - nullsFrom;
      while (--remaining >= 0)
        res.add(rdr.getInt());
      return res;
    }

    if (min.compareTo(min()) <= 0 && max.compareTo(max()) >= 0)
      return SortedIntSet.allValues((int)size);

    long low = 0;
    long high = size-1;
    while (low < high)
    {
      long middle = (low + high) >>> 1;
      rdr.position(8 + middle * keysHandler.itemSize());
      T item = keysHandler.read(rdr);
      if (comparator.compare(item, min) < 0)
        low = middle + 1;
      else if (comparator.compare(item, max) > 0)
        high = middle - 1;
      else
      {
        //here we found middle of the range, find exact boundaries
        low = findLowIndex(rdr, min, low, middle);
        high = findHighIndex(rdr, max, middle, high);
        int cnt = (int)(high-low+1);
        int[] result = new int[cnt];
        rdr.position(low * 4 + size * keysHandler.itemSize() + 8);
        for (int i = 0; i < cnt; ++i)
          result[i]  = rdr.getInt();
        return SortedIntSet.wrap(result);
      }
    }
    if (low == high)
    {
      rdr.position(8 + low * keysHandler.itemSize());
      T item = keysHandler.read(rdr);
      if (comparator.compare(item, min) >= 0 && comparator.compare(item, max) <= 0)
      {
        rdr.position(low * 4 + size * keysHandler.itemSize() + 8);
        return SortedIntSet.wrap(new int[] {rdr.getInt()});
      }
    }
    return SortedIntSet.empty();
  }

  private long findLowIndex(ReadBuffer rdr, T key, long low, long high)
  {
    while (low < high)
    {
      long middle = (low + high) >>> 1;
      rdr.position(8 + middle * keysHandler.itemSize());
      T item = keysHandler.read(rdr);

      if (comparator.compare(item, key) < 0)
        low = middle + 1;
      else
        high = middle;
    }
    return low;
  }

  private long findHighIndex(ReadBuffer rdr, T key, long low, long high)
  {
    while (low < high)
    {
      long middle = (low + high + 1) >>> 1;
      rdr.position(8 + middle * keysHandler.itemSize());
      T item = keysHandler.read(rdr);

      if (comparator.compare(item, key) <= 0)
        low = middle;
      else
        high = middle - 1;
    }
    return high;
  }

  @Override
  public void close() throws IOException
  {
    fc.close();
  }

  @Override
  public List<T> allKeys(boolean includeNull)
  {
    if (size == 0)
      return Collections.emptyList();

    ArrayList<T> allItems = new ArrayList<>((int)(size/4));
    try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
    {
      rdr.position(8);
      long expire = size - 1;
      T prev = keysHandler.read(rdr);
      if (includeNull || prev != null)
        allItems.add(prev);
      while (--expire >= 0)
      {
        T cur = keysHandler.read(rdr);
        if (Objects.equals(cur, prev))
          continue;
        if (includeNull || cur != null)
          allItems.add(cur);
        prev = cur;
      }
    }
    allItems.trimToSize();
    return allItems;
  }

  @Override
  public SortedIntSet valuesFor(T key)
  {
    try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
    {
      return doValuesFor(key, key, rdr);
    }
  }

  @Override
  public SortedIntSet valuesFor(Collection<T> keys)
  {
    try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
    {
      Iterator<T> it = keys.iterator();
      T val = it.next();
      SortedIntSet res = doValuesFor(val, val, rdr);
      while (it.hasNext())
      {
        val = it.next();
        res.union(doValuesFor(val, val, rdr));
      }
      return res;
    }
  }

  @Override
  public SortedIntSet valuesFor(Range<T>... ranges)
  {
    try (ReadBuffer rdr = new ReadBuffer(fc, buffSizeR))
    {
      SortedIntSet res = SortedIntSet.empty();
      for (Range<T> range: ranges)
        res.union(doValuesFor(range.getMin(), range.getMax(), rdr));
      return res;
    }
  }
}