package datalist.index;

import datalist.io.ChannelBuilder;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;
import datalist.stream.DBDataStreamer;
import datalist.types.CounterMap;
import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;
import java.util.List;
import java.util.Objects;

/**
 * This index loads all keys to RAM and reads only item indexes.
 * Useful for enum-like fields with few key values, but not recommended for mostly distinct fields.<br><br>
 * If storage fails, UncheckedIOException will be thrown.
 */
public class SetIndex<T extends Comparable<T>> implements Index<T>, AutoCloseable
{
  private static final int buffSizeR = Integer.getInteger("setindex_read_buffer_size", -1);
  private static final int buffSizeW = Integer.getInteger("setindex_write_buffer_size", -1);

  private final Comparator<KeyWithRange<T>> comparator = comparing(kl -> kl.key, nullsLast(naturalOrder()));
  private final ArrayList<KeyWithRange<T>> keys;
  private final FileChannel fc;
  private final DBDataStreamer<T> keysHandler;
  private int headerSize;

  public SetIndex(File storage, DBDataStreamer<T> keysHandler) throws IOException
  {
    fc = ChannelBuilder.forReadWrite(storage).build();
    this.keysHandler = keysHandler;
    keys = loadKeys();
  }

  @Override
  public void recreate(List<KeyToIndex<T>> values)
  {
    values.sort(KeyToIndex.keysComparator());
    try
    {
      fc.truncate(0);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
    try (WriteBuffer writer = new WriteBuffer(fc, buffSizeW))
    {
      writer.putInt(-1); //not calculated base offset
      CounterMap<T> allKeys = KeyToIndex.allKeys(values);
      writer.putInt(allKeys.size());
      keys.clear();
      keys.ensureCapacity(allKeys.size());
      for (T key: allKeys.keySet())
        keys.add(new KeyWithRange<>(key, -1, allKeys.getCount(key)));
      keys.sort(comparator);
      long offs = 0;
      for (KeyWithRange<T> key: keys)
      {
        key.offset = offs;
        keysHandler.write(writer, key.key);
        writer.putLong(key.offset);
        writer.putInt(key.count);
        offs += (key.count + 1) * 4; //4 is sizeof(int), index stores by int. +1 for hash (4 bytes too)
      }
      headerSize = (int)writer.position();

      int pos = 0;
      ArrayList<Integer> av = new ArrayList<>();
      for (KeyWithRange<T> key: keys)
      {
        pos = collectValues(values, key.key, av, pos);
        for (Integer val: av)
          writer.putInt(val);
        writer.putInt(av.hashCode());
      }

      writer.position(0);
      writer.putInt(headerSize);
    }
  }

  @Override
  public void clear()
  {
    try
    {
      fc.truncate(0);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
    keys.clear();
  }

  private int collectValues(List<KeyToIndex<T>> values, T key, ArrayList<Integer> av, int pos)
  {
    if (pos >= values.size())
      throw new IllegalStateException("End of collection reached, but key " + key + " is absend");
    av.clear();
    do
    {
      KeyToIndex<T> curKeyVal = values.get(pos);
      if (Objects.equals(key, curKeyVal.getKey()))
        av.add(curKeyVal.getIndex());
      else if (av.isEmpty())
        throw new IllegalStateException("Expected key " + key + ", but " + curKeyVal.getKey() + " found");
      else
        return pos;
    } while (++pos < values.size());
    return pos;
  }

  private ArrayList<KeyWithRange<T>> loadKeys()
  {
    try
    {
      if (fc.size() == 0)
        return new ArrayList<>(); //no keys, empty file

      try (ReadBuffer reader = new ReadBuffer(fc, buffSizeR))
      {
        headerSize = reader.getInt();

        int keysCount = reader.getInt();
        ArrayList<KeyWithRange<T>> res = new ArrayList<>(keysCount);
        int i = keysCount;
        while (--i >= 0)
        {
          T key = keysHandler.read(reader);
          res.add(new KeyWithRange<>(key, reader.getLong(), reader.getInt()));
        }
        return res;
      }
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public List<T> allKeys(boolean includeNull)
  {
    List<T> res = new ArrayList<>(keys.size());
    for (KeyWithRange<T> key: keys)
      if (includeNull || key.key != null)
        res.add(key.key);
    return res;
  }

  private SortedIntSet doValuesFor(T key, ReadBuffer reader)
  {
    KeyWithRange<T> superKey = new KeyWithRange<>(key, 0, 0);
    int idx = Collections.binarySearch(keys, superKey, comparator);
    if (idx < 0)
      return SortedIntSet.empty();
    KeyWithRange<T> info = keys.get(idx);

    reader.position((long)headerSize + info.offset);

    int[] res = new int[info.count];
    int req = info.count;
    for (int i = 0; i < req; ++i)
      res[i] = reader.getInt();
    final int hash = reader.getInt();
    if (Arrays.hashCode(res) != hash)
      throw new DatabaseBrokenError("Index storage corrupted");
    return SortedIntSet.wrap(res);
  }

  @Override
  public SortedIntSet valuesFor(T key)
  {
    try (ReadBuffer reader = new ReadBuffer(fc, buffSizeR))
    {
      return doValuesFor(key, reader);
    }
  }

  //private boolean allKeysRequired(Collection<T> rkeys)
  //{
  //  for (KeyWithRange<T> key: keys)
  //    if (!rkeys.contains(key.key))
  //      return false;
  //  return true;
  //}

  @Override
  public SortedIntSet valuesFor(Collection<T> keys)
  {
    //Code below does not work: it return keys count instead of values range
    //if (keys.size() >= this.keys.size())
    //  if (allKeysRequired(keys))
    //    return SortedIntSet.allValues(this.keys.size());

    try (ReadBuffer reader = new ReadBuffer(fc, buffSizeR))
    {
      SortedIntSet res = new SortedIntSet();
      for (T key: keys)
        res.union(doValuesFor(key, reader));
      return res.trim();
    }
  }

  @Override
  public SortedIntSet valuesFor(Range<T>... ranges)
  {
    List<T> allKeys = allKeys(false);
    List<T> reqKeys = new ArrayList<>(allKeys.size());
    for (T key: allKeys)
      for (Range<T> range: ranges)
        if (range.includeValue(key))
        {
          reqKeys.add(key);
          break;
        }
    return valuesFor(reqKeys);
  }

  @Override
  public void close()
  {
    try
    {
      fc.close();
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public SortedIntSet valuesFor(T min, T max)
  {
    ArrayList<T> usedVals = new ArrayList<>(keys.size());
    for (KeyWithRange<T> key: keys)
      if (key.key.compareTo(min) >= 0 && key.key.compareTo(max) <= 0)
        usedVals.add(key.key);
    return valuesFor(usedVals);
  }

  private static class KeyWithRange<T extends Comparable<T>>
  {
    public final T key;
    public long offset;
    public final int count;

    public KeyWithRange(T key, long offset, int count)
    {
      this.key = key;
      this.offset = offset;
      this.count = count;
    }

    @Override
    public String toString()
    {
      return key + " -> from " + offset + ", " + count + " items";
    }
  }
}
