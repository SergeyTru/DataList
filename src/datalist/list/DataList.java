package datalist.list;

import datalist.index.Index;
import datalist.index.KeyToIndex;
import datalist.io.ChannelBuilder;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.stream.DBDataStreamer;
import datalist.stream.FixedSizeDataStreamer;
import datalist.types.IntArray;
import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Nio files based implementation of the <tt>List</tt> interface.<br>
 * List optimized for write once read many (WORM). You may add and get items, but not modify or delete.<br>
 * List is thread-safe for get items if DBDataStreamer is thread safe.<br>
 * It might be {@link java.nio.BufferUnderflowException} errors if you clear DataList when other thread read data.<br><br>
 * If storage fails, UncheckedIOException will be thrown.
 * @author SergeyT
 * @param <T> 
 */
@SuppressWarnings("EqualsAndHashcode")
public class DataList<T> extends AbstractList<T> implements AutoCloseable
{
  private static final int buffSizeR = Integer.getInteger("database_read_buffer_size", 1024);
  private static final int buffSizeW = Integer.getInteger("database_write_buffer_size", 8192);
  private static final int cacheSize = Integer.getInteger("database_cache_size", 32);

  private final Database database;
  private final String tableName;

  private final DBDataStreamer<T> dataHandler;
  private final FileChannel fc;
  private final ItemOffsetList itemOffsets;
  private final ReadBuffer reader;

  private final List<IndexInfo> indexes = new ArrayList<>();
  private final List<DataListChangedListener<T>> listeners = new ArrayList<>();

  private final ConcurrentHashMap<Integer, T> linesCache = new ConcurrentHashMap<>(cacheSize);
  private volatile DataAppender appender;

  public DataList(Database db, String tableName, DBDataStreamer<T> dataHandler) throws IOException
  {
    if (db == null || tableName == null)
      throw new IllegalArgumentException();
    this.database = db;
    this.tableName = tableName;
    this.fc = ChannelBuilder.forReadWrite(db.getDataFile(tableName)).build();
    this.reader = new ReadBuffer(fc, buffSizeR);
    if (dataHandler instanceof FixedSizeDataStreamer)
      itemOffsets = null;
    else
      itemOffsets = new ItemOffsetList(db.getItemOffsetsFile(tableName).toPath());
    this.dataHandler = dataHandler;
    this.listeners.add(new DataListChangedListener<T>(){
      @Override
      public void cleaned(DataList<T> list)
      {
        for (IndexInfo index: indexes)
          index.index.clear();
      }

      @Override
      @SuppressWarnings("unchecked")
      public void dataAdded(DataList<T> list, int from, int till)
      {
        int dataSize = size();
        int i = indexes.size();
        List<List<KeyToIndex>> allValues = new ArrayList<>(i);
        while (--i >= 0)
          allValues.add(new ArrayList<>(dataSize));
        for (int lineIdx = 0; lineIdx < dataSize; ++lineIdx)
        {
          T line = get(lineIdx);
          i = indexes.size();
          while (--i >= 0)
            allValues.get(i).add(new KeyToIndex((Comparable)indexes.get(i).mapper.apply(line), lineIdx));
        }
        i = indexes.size();
        while (--i >= 0)
        {
          indexes.get(i).index.recreate(allValues.get(i));
          allValues.set(i, null); //collect garbage as soon as possible
        }
      }
    });
  }

  public void addListener(DataListChangedListener<T> lstn)
  {
    this.listeners.add(lstn);
  }

  @SuppressWarnings("unchecked")
  public <U extends Comparable<U>> DataList<T> withIndex(Index<U> index, Function<T, U> mapper, boolean indexNulls)
  {
    indexes.add(new IndexInfo(index, mapper, indexNulls));
    return this;
  }

  public Database getDatabase()
  {
    return database;
  }

  public String getTableName()
  {
    return tableName;
  }

  @Override
  public void close() throws IOException
  {
    fc.close();
    if (itemOffsets != null)
      itemOffsets.close();
  }

  @Override
  public T get(int idx)
  {
    if (appender != null)
      throw new IllegalStateException("Append mode");

    T item = linesCache.get(idx);
    if (item != null)
      return item;

    if (idx >= size())
      throw new IndexOutOfBoundsException("Index: "+idx+", size: "+size());

    synchronized (this)
    {
      if (itemOffsets != null)
        item = readVarySize(idx);
      else
        item = readFixedSize(idx);

      if (linesCache.size() == cacheSize)
        linesCache.clear();
      linesCache.put(idx, item);
    }

    return item;
  }

  private T readVarySize(int idx) throws DatabaseBrokenError
  {
    reader.position(itemOffsets.from(idx));
    T res = dataHandler.read(reader);
    if (reader.position() != itemOffsets.till(idx))
      throw new DatabaseBrokenError("Item size");
    return res;
  }

  private T readFixedSize(int idx) throws DatabaseBrokenError
  {
    int size = ((FixedSizeDataStreamer)dataHandler).itemSize();
    reader.position(idx * size);
    T res = dataHandler.read(reader);
    if (reader.position() != (idx+1) * size)
      throw new DatabaseBrokenError("Item size");
    return res;
  }

  @Override
  public int size()
  {
    if (appender == null)
      return internalSize();
    else if (appender.thisThread == Thread.currentThread())
      return internalSize() + appender.idxPos;
    else
      throw new IllegalStateException("Only appender thread could opearate with list at append mode");
  }

  private int internalSize() throws UncheckedIOException
  {
    if (itemOffsets != null)
      return itemOffsets.size();
    else
      try
      {
        return (int) (fc.size() / ((FixedSizeDataStreamer)dataHandler).itemSize());
      }
      catch(IOException ex)
      {
        throw new UncheckedIOException(ex);
      }
  }

  @Override
  public boolean add(T e)
  {
    return addAll(Collections.singleton(e));
  }

  @Override
  public synchronized boolean addAll(Collection<? extends T> c)
  {
    if (c.isEmpty())
      return true;

    DataAppender app = appender;
    try
    {
      if (app == null)
        app = new DataAppender(c.size());
      for (T elem: c)
        app.addItem(elem);
      return true;
    }
    finally
    {
      if (app != appender)
        app.close();
    }
  }

  @Override
  public void clear()
  {
    if (appender != null)
      throw new IllegalStateException("Append mode");

    try
    {
      fc.truncate(0);
      reader.invalidateBuffer();
      if (itemOffsets != null)
        itemOffsets.clear();
      linesCache.clear();
      for (DataListChangedListener<T> listener: listeners)
        listener.cleaned(this);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public int hashCode()
  {
    return tableName.hashCode();
  }

  public DataAppender getAppender(int chunkSize)
  {
    if (appender != null)
      throw new IllegalStateException("Appender already opened!");
    return appender = new DataAppender(chunkSize);
  }

  public List<Index> getIndexes()
  {
    return indexes.stream().map(x -> x.index).collect(Collectors.toList());
  }

  public boolean hasIndex(Index<?> index)
  {
    return indexes.stream().anyMatch(x -> x.index.equals(index));
  }

  public <T extends Comparable<T>> Where where(Index<T> index, T value)
  {
    return new Where(this).and(index, value);
  }

  public <T extends Comparable<T>> Where where(Index<T> index, T min, T max)
  {
    return new Where(this).and(index, min, max);
  }

  public <T extends Comparable<T>> Where where(Index<T> index, Collection<T> values)
  {
    return new Where(this).and(index, values);
  }

  public <T extends Comparable<T>> Where where(Index<T> index, Range<T>... ranges)
  {
    return new Where(this).and(index, ranges);
  }

  public <T extends Comparable<T>> Where where(Index<T> index, Predicate<T> condition, boolean includeNull)
  {
    return new Where(this).and(index, condition, includeNull);
  }

  private void reindex(int oldCount)
  {
    if (this.size() > oldCount)
    {
      int fromPos = oldCount + 1;
      for (DataListChangedListener<T> listener: listeners)
        listener.dataAdded(this, fromPos, this.size());
    }
  }

  public List<T> sublist(IntArray indexes)
  {
    return new AbstractList<T>()
    {
      @Override
      public T get(int index)
      {
        return DataList.this.get(indexes.get(index));
      }

      @Override
      public int size()
      {
        return indexes.size();
      }
    };
  }

  @SuppressWarnings("unchecked")
  private SortedIntSet allIndexesFor(T obj)
  {
    SortedIntSet res = SortedIntSet.allValues(size());
    for (IndexInfo indexInfo: indexes)
    {
      res.intersect(indexInfo.allForObject(obj));
      if (res.isEmpty())
        return res;
    }     
    return res;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int indexOf(Object o)
  {
    try
    {
      for (Integer idx: allIndexesFor((T) o))
        if (Objects.equals(o, get(idx)))
          return idx;
      return -1;
    }
    catch (ClassCastException ex)
    {
      return -1;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public int lastIndexOf(Object o)
  {
    SortedIntSet idx;
    try
    {
      idx = allIndexesFor((T)o);
    }
    catch (ClassCastException ex)
    {
      return -1;
    }
    for (int i = idx.size() - 1; i >= 0; --i)
      if (Objects.equals(o, get(idx.get(i))))
        return idx.get(i);
    return -1;
  }

  public class DataAppender implements AutoCloseable
  {
    private final Thread thisThread;
    private final WriteBuffer dataWriter;
    private final long[] idxs;
    private int idxPos;
    private final int oldCount;

    DataAppender(int idxBuffSize)
    {
      dataWriter = new WriteBuffer(fc, buffSizeW);
      idxs = itemOffsets != null? new long[idxBuffSize] : null;
      thisThread = Thread.currentThread();
      idxPos = 0;
      oldCount = size();
    }

    public int addItem(T item)
    {
      beforeAdd();

      dataHandler.write(dataWriter, item);
      if (idxs != null)
        idxs[idxPos] = dataWriter.position();
      return internalSize() + idxPos++;
    }

    private void beforeAdd()
    {
      if (thisThread != Thread.currentThread())
        throw new IllegalStateException("Only one thread may update data");
      if (idxs != null && idxPos == idxs.length)
      {
        dataWriter.flush(); //indexes should always point to data
        itemOffsets.add(idxs, idxPos);
        idxPos = 0;
      }
    }

    @Override
    public void close()
    {
      dataWriter.close();
      if (idxPos > 0)
      {
        if (itemOffsets != null)
          itemOffsets.add(idxs, idxPos);
      }
      appender = null;
      reindex(oldCount);
    }
  }

  private class IndexInfo<U extends Comparable<U>>
  {
    final Index<U> index;
    final Function<T, U> mapper;

    public IndexInfo(Index<U> index, Function<T, U> mapper, boolean indexNulls)
    {
      this.index = index;
      this.mapper = mapper;
    }
    
    public SortedIntSet allForObject(T object)
    {
      return index.valuesFor(mapper.apply(object));
    }
  }
}