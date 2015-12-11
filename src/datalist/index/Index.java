package datalist.index;

import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.util.Collection;
import java.util.List;

/**
 * All DataList indexes implements this interface.
 * @param <T> the type of index values. Should be {@link java.lang.Comparable} for binary search, binary trees, etc.
 * @author SergeyT
 */
public interface Index<T extends Comparable<T>>
{
  /**
   * Recreates index by list 'values'. List would be sorted by {@link KeyToIndex.keysComparator}.<br><br>
   * <strong>You must guarantee that no one reads the data in the process of rebuild index.</strong>
   */
  public void recreate(List<KeyToIndex<T>> values);

  /** Clear whole index */
  public void clear();

  /** Returns all keys
   * @param includeNull helps you avoid NullPointerException (removes null value)
   */
  public List<T> allKeys(boolean includeNull);

  /** Returns all indexes for key */
  SortedIntSet valuesFor(T key);

  /** Returns all indexes for any key in collection sorted ascending */
  SortedIntSet valuesFor(Collection<T> keys);

  /** Returns all indexes for range (inclusive) */
  SortedIntSet valuesFor(T min, T max);

  /** Returns all indexes for ranges (inclusive) */
  SortedIntSet valuesFor(Range<T>... ranges);
}
