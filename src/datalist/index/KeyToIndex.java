package datalist.index;

import datalist.types.CounterMap;
import java.util.Collection;
import java.util.Comparator;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

/**
 * Helper class for {@link Index} to keep row numbers for each key.
 * Allows to simplify Index rebuild: {@link datalist.list.DataList}.reindex() collect values for all
 * indexes and then call {@link datalist.index.Index}.recreate() for each of them. List items are read only once.
 * @param <T> is index type
 * @author SergeyT
 */
public class KeyToIndex<T extends Comparable<T>>
{
  private final T key;
  private final int index;

  public KeyToIndex(T key, int index)
  {
    this.key = key;
    this.index = index;
  }

  @Override
  public String toString()
  {
    return key + " -> " + index;
  }

  public T getKey()
  {
    return key;
  }

  public int getIndex()
  {
    return index;
  }

  /** Sort items by key, put nulls at the end. For each key, sort items by row index */
  public static <T extends Comparable<T>> Comparator<KeyToIndex<T>> keysComparator()
  {
    Comparator<KeyToIndex<T>> comp = comparing(kl -> kl.key, nullsLast(naturalOrder()));
    return comp.thenComparingInt(kl -> kl.index);
  }

  /** Calculate count for each key in list. Call {@link CounterMap#keySet() CounterMap.keySet()} for all keys set */
  public static <T extends Comparable<T>> CounterMap<T> allKeys(Collection<KeyToIndex<T>> values)
  {
    CounterMap<T> res = new CounterMap<>();
    for (KeyToIndex<T> val: values)
      res.increment(val.key);
    return res;
  }
}
