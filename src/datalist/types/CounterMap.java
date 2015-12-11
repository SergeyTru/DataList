package datalist.types;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Utility class to count the number of each key. Keeps insertion order */
public class CounterMap<K>
{
  Map<K, Counter> innerMap;

  public CounterMap()
  {
    innerMap = new LinkedHashMap<>();
  }

  private static class Counter
  {
    private int cnt = 1;
    public int get() {return cnt;}
    public void addFrom(Counter from) {cnt += from.cnt;}
    @Override public String toString(){return String.valueOf(cnt);}
  }

  public void increment(K value)
  {
    Counter cntr = innerMap.get(value);
    if (cntr == null)
      innerMap.put(value, new Counter());
    else
      cntr.cnt++;
  }

  /**
   * If counter >0 it decremented, otherwise it set to 0
   * @return true if counter still greater than 0
   */
  public boolean decrementOrZero(K value)
  {
    Counter cntr = innerMap.get(value);
    if (cntr == null)
      return false;
    if (--cntr.cnt > 0)
      return true;
    innerMap.remove(value);
    return false;
  }

  public boolean remove(K value)
  {
    return innerMap.remove(value) != null;
  }

  public int getCount(K value)
  {
    Counter cntr = innerMap.get(value);
    return (cntr==null)?0:cntr.cnt;
  }

  /**
   * Check if there are one or more items with count <b>cnt</b> and more.
   * Works faster than search maximum and check if it bigger
   * @param cnt - how many items should be
   */
  public boolean hasCntAtLeast(int cnt)
  {
    return innerMap.values().stream().anyMatch(x -> x.cnt >= cnt);
  }

  /**
   * Remove all items with the count less than <b>cnt</b>
   * @param cnt - minimum count to stay in set
   * @return true if one or more items removed, false if no items removed
   */
  public boolean removeLessThan(int cnt)
  {
    boolean result = false;
    for (Iterator<Map.Entry<K, Counter>> it = innerMap.entrySet().iterator(); it.hasNext();)
    {
      Map.Entry<K, CounterMap.Counter> item = it.next();
      if (item.getValue().cnt < cnt)
      {
        it.remove();
        result = true;
      }
    }
    return result;
  }

  public int getMaxCount()
  {
    int maxv = 0;
    for (Counter item: innerMap.values())
      if (item.cnt > maxv)
        maxv = item.cnt;
    return maxv;
  }

  /**
   * Find element with maximum count
   * @param last is true for last added item, false for first
   * @return null only if empty
   */
  public K getBiggest()
  {
    int maxCnt = 0;
    K bestVal = null;
    for (Map.Entry<K, Counter> pair: innerMap.entrySet())
    {
      int cur = pair.getValue().cnt;
      if (cur > maxCnt)
      {
        maxCnt = cur;
        bestVal = pair.getKey();
      }
    }
    return bestVal;
  }

  public CounterMap<K> addAll(Collection<? extends K> items)
  {
    items.stream().forEach(item -> increment(item));
    return this;
  }

  public Set<K> keySet()
  {
    return innerMap.keySet();
  }

  public int size()
  {
    return innerMap.size();
  }

  public boolean isEmpty()
  {
    return innerMap.isEmpty();
  }

  public void clear()
  {
    innerMap.clear();
  }

  public boolean containsKey(K key)
  {
    return innerMap.containsKey(key);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<K, Counter> item: innerMap.entrySet())
    {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append(item.getKey()).append(" x ").append(item.getValue());
    }
    return sb.toString();
  }
}