package datalist.master_details;

import datalist.index.Index;
import datalist.list.DataList;
import datalist.list.DataListChangedListener;
import datalist.types.IntArray;
import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * {@link DataList} implementation of <a href="https://en.wikipedia.org/wiki/One-to-many_%28data_model%29">one-to-many</a>
 * relation. Extracting common information to master table allow to reduce used storage space, index size.<br>
 * Just use two DataLists from one {@link datalist.list.Database} and "itemGenerator" function to merge item from two tables.<br>
 * Master list contains "one" items, details list contains "many" items.<br><br>
 * MasterDetailsList is thread-safe if lists is thread-safe (DataStreamers are thread-safe) and itemGenerator is thread-safe.<br><br>
 * <pre>
 * Example:
 * {@code  DataList<Author> masterList = new DataList<>(db, "authors", AuthorsStreamer.INSTANCE);
 *   DataList<StreamedBook> detailsList = new DataList<>(db, "books", BooksStreamer.INSTANCE);
 *   MasterDetailsList<Author, StreamedBook, Book> mainList = new MasterDetailsList<>(masterList, detailsList, (m, d) -> new Book(m, d));
 *   //filter both lists:
 *   MasterDetailsWhere where = mainList.where(year, 1990, 2015).and(genre, fantasy);
 *   MasterDetailsIndexes idx = where.getIndexes(); //indexes at both tables
 *   System.out.printf("Found %d items at master list and %d items at details list%n", idx.getMasterIndexes().size(), idx.getDetailsIndexes().size());
 *   //Get filtered lists:
 *   List<Author> masterSubList = masterList.sublist(idx.getMasterIndexes());
 *   List<Book> detailsSubList = mainList.sublist(idx.getDetailsIndexes());
 * }</pre>
 * @author SergeyT
 * @param <M> type of items at master list
 * @param <S> type of items at details list
 * @param <T> produced items type (usualy produced by merging master and detail lists items)
 * @see MasterDetailsWhere
 * @see datalist.table.MasterDetailsTextSearch
 * @see MasterDetailsAppender
 */
public class MasterDetailsList<M, S, T> extends AbstractList<T>
{
  private final DataList<M> master;
  private final DataList<S> details;
  private final BiFunction<M, S, T> itemGenerator;
  private final CrossIndex crossIndex;

  private static final int cacheSize = Integer.getInteger("database_cache_size", 32);
  private final ConcurrentHashMap<Integer, T> linesCache = new ConcurrentHashMap<>(cacheSize);

  public MasterDetailsList(DataList<M> master, DataList<S> details, BiFunction<M, S, T> itemGenerator) throws IOException
  {
    if (master == null || details == null || itemGenerator == null)
      throw new IllegalArgumentException();
    if (master.getDatabase() != details.getDatabase())
      throw new IllegalArgumentException("Lists should be owned by one database");

    this.master = master;
    this.details = details;
    this.itemGenerator = itemGenerator;
    this.crossIndex = new CrossIndex(details.getDatabase().getCrossIndexFile(details.getTableName(), master.getTableName()));
    if (crossIndex.size() != details.size())
      throw new IllegalStateException("CrossIndex " + details.getTableName() + " to " + master.getTableName() + " has different size");
    
    this.master.addListener(new DataListChangedListener<M>()
    {
      @Override
      public void cleaned(DataList<M> list)
      {
        MasterDetailsList.this.details.clear(); //cleanup all
      }

      @Override
      public void dataAdded(DataList<M> list, int from, int till)
      {
      }
    });
    this.details.addListener(new DataListChangedListener<S>()
    {
      @Override
      public void cleaned(DataList<S> list)
      {
        crossIndex.clean();
      }

      @Override
      public void dataAdded(DataList<S> list, int from, int till)
      {
        if (till != crossIndex.size())
          throw new UnsupportedOperationException("Use master/details appender");
      }
    });
  }

  public DataList<M> getMasterList()
  {
    return master;
  }

  public DataList<S> getDetailsList()
  {
    return details;
  }

  public CrossIndex getCrossIndex()
  {
    return crossIndex;
  }

  @Override
  public int size()
  {
    return details.size();
  }

  @Override
  public boolean isEmpty()
  {
    return details.isEmpty();
  }

  @Override
  public boolean add(T e)
  {
    throw new UnsupportedOperationException("Use master/details appender");
  }

  @Override
  public void add(int index, T element)
  {
    throw new UnsupportedOperationException("Use master/details appender");
  }

  @Override
  public boolean addAll(Collection<? extends T> c)
  {
    throw new UnsupportedOperationException("Use master/details appender");
  }

  @Override
  public void clear()
  {
    //first variant: clear both lists
    master.clear();
    //second variant: clear details only
    //details.clear();
  }

  @Override
  public T get(int index)
  {
    T item = linesCache.get(index);
    if (item != null)
      return item;

    item = itemGenerator.apply(master.get(crossIndex.manyToOne(index)), details.get(index));

    if (linesCache.size() == cacheSize)
      linesCache.clear();
    linesCache.put(index, item);

    return item;
  }

  public List<T> sublist(IntArray indexes)
  {
    return new AbstractList<T>()
    {
      @Override
      public T get(int index)
      {
        return MasterDetailsList.this.get(indexes.get(index));
      }

      @Override
      public int size()
      {
        return indexes.size();
      }
    };
  }

  public <T extends Comparable<T>> MasterDetailsWhere where(Index<T> index, T value)
  {
    return new MasterDetailsWhere(master, details, crossIndex).and(index, value);
  }

  public <T extends Comparable<T>> MasterDetailsWhere where(Index<T> index, T min, T max)
  {
    return new MasterDetailsWhere(master, details, crossIndex).and(index, min, max);
  }

  public <T extends Comparable<T>> MasterDetailsWhere where(Index<T> index, Collection<T> values)
  {
    return new MasterDetailsWhere(master, details, crossIndex).and(index, values);
  }

  public <T extends Comparable<T>> MasterDetailsWhere where(Index<T> index, Predicate<T> condition, boolean includeNull)
  {
    return new MasterDetailsWhere(master, details, crossIndex).and(index, condition, includeNull);
  }
  
  public <T extends Comparable<T>> MasterDetailsWhere where(Index<T> index, Range<T>... ranges)
  {
    return new MasterDetailsWhere(master, details, crossIndex).and(index, ranges);
  }
  
  public MasterDetailsIndexes allValues()
  {
    return new MasterDetailsIndexes(SortedIntSet.allValues(master.size()), SortedIntSet.allValues(details.size()));
  }
}
