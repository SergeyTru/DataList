package datalist.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Predicate;

/** SortedIntSet keep items sorted to simplify union/intersersection */
public class SortedIntSet implements IntArray
{
  private int[] data;
  private int size;
  private boolean sorted;

  public SortedIntSet(int initialCapacity)
  {
    data = new int[initialCapacity];
  }

  public SortedIntSet()
  {
    this(128);
  }

  public SortedIntSet(Collection<Integer> c)
  {
    this(c.size());
    for (Integer elem: c)
      data[size++] = elem;
  }

  public SortedIntSet(int... c)
  {
    this(c.length);
    for (Integer elem: c)
      data[size++] = elem;
  }

  private SortedIntSet(int[] data, int size, boolean isSorted)
  {
    this.data = data;
    this.size = size;
    sorted = isSorted;
  }

  public static SortedIntSet wrap(int[] data)
  {
    return new SortedIntSet(data, data.length, false);
  }

  public SortedIntSet copy()
  {
    SortedIntSet res;
    if (data != null)
      res = new SortedIntSet(Arrays.copyOf(data, size), size, sorted);
    else
      res = new SortedIntSet(null, size, sorted);
    res.size = size;
    return res;
  }

  public static SortedIntSet empty()
  {
    return new SortedIntSet(null, 0, true);
  }

  public static SortedIntSet allValues(int size)
  {
    return new SortedIntSet(null, size, true);
  }

  public boolean allValuesList()
  {
    return size > 0 && data == null;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    if (data == null)
      return new Iterator<Integer>()
      {
        int pos;

        @Override
        public boolean hasNext()
        {
          return pos < size;
        }

        @Override
        public Integer next()
        {
          return pos++;
        }
      };
    else
      return new Iterator<Integer>()
      {
        int pos;

        @Override
        public boolean hasNext()
        {
          return pos < size;
        }

        @Override
        public Integer next()
        {
          return data[pos++];
        }
      };
  }

  private void sortDeduplicate()
  {
    if ((data == null || sorted) || size <= 0)
      return;

    Arrays.sort(data, 0, size);
    
    //deduplicate
    int i = 0;
    int w = 1;
    int pre = data[0];
    while (++i < size)
      if (data[i] != pre)
      {
        data[w++] = data[i];
        pre = data[i];
      }
    size = w;

    sorted = true;
  }

  public boolean removeIf(Predicate<Integer> filter)
  {
    if (size == 0)
      return false;

    if (data == null)
    {
      int w = 0;
      int[] ndata = new int[size];
      for (int i = 0; i < size; ++i)
        if (!filter.test(i))
          ndata[w++] = i;

      if (w == 0)
      {
        size = 0;
      }
      else if (w < size)
      {
        data = ndata;
        size = w;
      }
      else
        return false;

      return true;
    }

    int w = 0;
    for (int i = 0; i < size; ++i)
      if (!filter.test(data[i]))
        data[w++] = data[i];
    boolean res = w < size;
    size = w;
    return res;
  }

  public boolean removeAll(SortedIntSet arr)
  {
    if (size == 0 || arr.size == 0)
      return false;
    if (this == arr || arr.data == null)
    {
      size = 0;
      data = null;
      return true;
    }

    sortDeduplicate();
    arr.sortDeduplicate();

    if (data == null) //any value from [0, size)
      return this.removeIf(x -> arr.contains(x)); //rewrite when speed is critical
    else
      return removeAll_main(arr);
  }

  private boolean removeAll_main(SortedIntSet arr)
  {
    int pos1 = 0;
    int pos2 = 0;
    int len1 = size;
    int len2 = arr.size;
    int item1 = data[pos1];
    int item2 = arr.data[pos2];
    while (item1 < item2 && ++pos1 < len1)
      item1 = data[pos1];
    int w = pos1;
    boolean ended = false;
    while (!ended && pos1 < len1)
    {
      while (item1 < item2)
      {
        data[w++] = item1;
        if (++pos1 < len1)
          item1 = data[pos1];
        else
        {
          ended = true;
          break; //item2 bigger than last item1, keep last
        }
      }
      if (!ended)
        while (item1 > item2)
        {
          if (++pos2 < len2)
            item2 = arr.data[pos2];
          else
          {
            System.arraycopy(data, pos1, data, w, len1 - pos1);
            w += len1 - pos1;
            ended = true;
            break;
          }
        }
      if (!ended)
        while (item1 == item2 && ++pos1 < len1)
        {
          if (++pos2 < len2)
          {
            item1 = data[pos1];
            item2 = arr.data[pos2];
          }
          else
          {
            System.arraycopy(data, pos1, data, w, len1 - pos1);
            w += len1 - pos1;
            ended = true;
            break;
          }
        }
    }
    size = w;
    return w < len1;
  }

  public boolean intersect(SortedIntSet arr)
  {
    if (this == arr)
      return false;
    if (size == 0)
      return false;
    if (arr.size == 0)
    {
      makeEmpty();
      return true;
    }
    if (arr.data == null) //arr has all values, nothing to remove
    {
      if (this.biggest() < arr.size)
        return false;
      else
        return removeIf(x -> x >= arr.size);
    }
    if (data == null) //this list contains all values, arr not empty and not full, copy values from arr
    {
      int oldSize = size;
      data = Arrays.copyOf(arr.data, arr.size);
      size = arr.size;
      sorted = arr.sorted;
      removeIf(x -> x >= oldSize);
      return true;
    }

    //usual arrays with filled data[]
    sortDeduplicate();
    arr.sortDeduplicate();
    int w = 0;
    int pos2 = 0;
    int idx;
    for (idx = 0; idx < size; ++idx)
    {
      int item = data[idx];
      while (pos2 < arr.size && arr.data[pos2] < item)
        ++pos2;
      if (pos2 >= arr.size)
      {
        size = w;
        return true;
      }
      if (arr.data[pos2] == item)
        data[w++] = data[idx];
    }
    if (w == size)
      return false;
    size = w;
    return true;
  }

  private int biggest()
  {
    if (data == null)
      return size - 1;
    if (sorted)
      return data[size - 1];
    else
      return Arrays.stream(data, 0, size).max().getAsInt();
  }

  public void union(SortedIntSet arr)
  {
    if (this == arr || arr.isEmpty())
      return;
    if (allValuesList())
    {
      if (this.size > arr.biggest())
        return;
      unpackData(arr.size);
    }
    if (arr.allValuesList() && arr.size <= biggest())
    {
      arr.unpackData(0);
      if (data == null)
        unpackData(arr.size);
    }
    else if (isEmpty() || arr.allValuesList())
    {
      //SpecialState oldState = state;
      data = arr.data == null? null : Arrays.copyOf(arr.data, arr.data.length);
      size = arr.size;
      sorted = arr.sorted;
      return;
    }

    sorted = false;
    if (size + arr.size > data.length)
      data = Arrays.copyOf(data, size + arr.size);
    System.arraycopy(arr.data, 0, data, size, arr.size);
    size += arr.size;
  }

  private void makeEmpty()
  {
    size = 0;
    data = null;
  }

  public void clear()
  {
    size = 0;
    data = null;
  }

  public boolean remove(int val)
  {
    if (size == 0)
      return false;
    else if (data == null)
      return (val < size) && removeIf(i -> i == val);

    sortDeduplicate();
    int idx = Arrays.binarySearch(data, 0, size, val);
    if (idx < 0)
      return false;

    System.arraycopy(data, idx + 1, data, idx, size - idx - 1);
    --size;
    return true;
  }

  public void add(int value)
  {
    if (allValuesList())
    {
      if (value > size)
      {
        unpackData(1);
        data[size++] = value;
      }
      else if (value == size)
        ++size;
      return;
    }
    if (size == 0)
    {
      data = new int[128];
    }
    else if (size == data.length)
    {
      sortDeduplicate(); //sometimes it remove duplicates
      if (size == data.length)
        data = Arrays.copyOf(data, size * 2);
    }
    if (sorted && Arrays.binarySearch(data, 0, size, value) >= 0)
      return;
    data[size++] = value;
    sorted = false;
  }

  private void unpackData(int additionalItems)
  {
    int[] newData = new int[additionalItems + size];
    for (int i = size - 1; i >= 0; --i)
      newData[i] = i;
    data = newData;
    sorted = true;
  }

  public void addAll(Collection<Integer> values)
  {
    if (values.isEmpty())
      return;
    if (allValuesList())
    {
      if (!values.stream().anyMatch(x -> x >= size))
        return;
      unpackData(values.size());
    }
    else if (data == null)
    {
      data = new int[Math.max(values.size(), 128)];
    }
    else if (size + values.size() >= data.length)
    {
      sortDeduplicate(); //sometimes it remove duplicates
      if (size + values.size() >= data.length)
        data = Arrays.copyOf(data, Math.max(size + values.size(), size * 2));
    }
    for (Integer val: values)
      data[size++] = val;
    sorted = false;
  }

  @Override
  public int get(int index)
  {
    if (data == null)
    {
      if (index < size)
        return index;
      else
        throw new IllegalStateException("Collection size is " + size + ", no " + index + "th element");
    }

    if (!sorted)
      sortDeduplicate();
    return data[index];
  }

  @Override
  public boolean contains(int val)
  {
    sortDeduplicate();
    if (data != null)
      return Arrays.binarySearch(data, 0, size, val) >= 0;
    else
      return val < size;
  }

  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  @Override
  public int size()
  {
    sortDeduplicate();
    return size;
  }

  public SortedIntSet trim()
  {
    if (data != null)
    {
      if (size == 0)
        data = null;
      else
      {
        sortDeduplicate(); //removes duplicates
        if (data.length * 3./4. > size)
          data = Arrays.copyOf(data, size);
      }
    }
    return this;
  }

  @Override
  public int hashCode()
  {
    return size;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null || getClass() != obj.getClass())
      return false;
    final SortedIntSet other = (SortedIntSet) obj;
    sortDeduplicate();
    other.sortDeduplicate();
    if (size != other.size)
      return false;
    if (size == 0)
      return true;

    //check if one array is fullfilled till N (data=null) and other is [0..N-1]
    if (data == null || other.data == null)
      return containsAllValues(data, size) || containsAllValues(other.data, size);

    if (!this.sorted || !other.sorted)
    {
      sortDeduplicate();
      other.sortDeduplicate();
      if (size != other.size)
        return false;
    }

    final int[] curData = data;
    final int[] othData = other.data;
    for (int i = size-1; i >= 0; --i)
      if (curData[i] != othData[i])
        return false;
    return true;
  }

  private boolean containsAllValues(int[] array, int size)
  {
    if (array == null)
      return true;
    //array already sorted, deduplicated. Return "true" if array is [0..size-1]
    for (int i = size - 1; i >= 0; --i)
      if (array[i] != i)
        return false;
    return true;
  }

  @Override
  public String toString()
  {
    final List<Range<Integer>> ranges = getRanges();
    if (ranges.isEmpty())
      return "[]";

    StringJoiner sj = new StringJoiner(", ", "[", "]");
    for (Range<Integer> range: ranges)
      if (range.getMin().equals(range.getMax()))
        sj.add(range.getMin().toString());
      else if (range.getMin() + 1 == range.getMax())
      {
        sj.add(range.getMin().toString());
        sj.add(range.getMax().toString());
      }
      else
        sj.add(range.getMin().toString() + "-" + range.getMax().toString());
    if (allValuesList())
      return sj.toString() + " (all)";
    else
      return sj.toString();
  }

  public List<Range<Integer>> getRanges()
  {
    if (size == 0)
      return Collections.emptyList();
    if (data == null)
      return Collections.singletonList(new Range<>(0, size-1));

    sortDeduplicate();
    List<Range<Integer>> res = new ArrayList<>();
    int strt = data[0];
    int prev = strt;
    int sz = size, cur;
    for (int i = 1; i < sz; ++i)
    {
      cur = data[i];
      if (cur != prev + 1)
      {
        res.add(new Range<>(strt, prev));
        strt = cur;
      }
      prev = cur;
    }
    res.add(new Range<>(strt, prev));
    return res;
  }
}