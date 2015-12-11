package datalist.list;

import datalist.index.Index;
import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Helping class to filter data.<br>
 * For example:<br>
 * guestsList.where(age, 20, 30).and(transport, set -> set.contains(Transport.CAR)
 * return all row numbers of guests list with specific age range and lambda condition for transport type.<br>
 * <br>
 * Note that only index files used, without any reads from DataList (except cases when Index use DataList)<br>
 * <br>
 * Use 'intersect' to filter data at master and slave list.<br>
 * <br>
 * After all, use getIndexes() to get all indexes
 */
public class Where
{
  private final DataList<?> list;
  private final SortedIntSet arr;

  public Where(DataList<?> data)
  {
    this.list = data;
    this.arr = SortedIntSet.allValues(data.size());
  }

  public <T extends Comparable<T>> Where and(Index<T> index, T value)
  {
    if (!indexApplyable(index))
      throw new IllegalArgumentException("Index not applyable for " + list.getTableName());
    if (!arr.isEmpty())
      arr.intersect(index.valuesFor(value));
    return this;
  }

  public <T extends Comparable<T>> Where and(Index<T> index, T min, T max)
  {
    if (!indexApplyable(index))
      throw new IllegalArgumentException("The index not applyable for " + list.getTableName());
    if (!arr.isEmpty())
      arr.intersect(index.valuesFor(min, max));
    return this;
  }

  public <T extends Comparable<T>> Where and(Index<T> index, Collection<T> values)
  {
    if (!indexApplyable(index))
      throw new IllegalArgumentException("The index not applyable for " + list.getTableName());
    if (!arr.isEmpty() && values != null)
      arr.intersect(index.valuesFor(values));
    return this;
  }

  public <T extends Comparable<T>> Where and(Index<T> index, Predicate<T> condition, boolean includeNull)
  {
    if (!indexApplyable(index))
      throw new IllegalArgumentException("The index not applyable for " + list.getTableName());
    if (!arr.isEmpty())
    {
      ArrayList<T> keys = new ArrayList<>(index.allKeys(includeNull));
      if (!keys.removeIf(x -> !condition.test(x)))
        return this; //all keys

      SortedIntSet internal = new SortedIntSet();
      for (T key: keys)
        internal.union(index.valuesFor(key));
      arr.intersect(internal);
    }
    return this;
  }

  public <T extends Comparable<T>> Where and(Index<T> index, Range<T>... ranges)
  {
    if (!indexApplyable(index))
      throw new IllegalArgumentException("The index not applyable for " + list.getTableName());
    if (!arr.isEmpty() && ranges != null)
      arr.intersect(index.valuesFor(ranges));
    return this;
  }

  public Where andIndexInList(SortedIntSet internal)
  {
    arr.intersect(internal);
    return this;
  }

  public final boolean indexApplyable(Index<?> index)
  {
    return list.hasIndex(index);
  }

  public SortedIntSet getIndexes()
  {
    return arr.trim();
  }

  public boolean isEmpty()
  {
    return arr.isEmpty();
  }

  @Override
  public String toString()
  {
    return "List: " + list.getTableName() + ", items indexes: " + arr;
  }
}
