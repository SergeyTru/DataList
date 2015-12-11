package datalist.table;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Basic implementation for {@link TableRow} that stores all fields in array.<br>
 * Constructor is protected and should be called from extend class with same 'reqSize' for same table
 * @author SergeyT
 */
public class SimpleTableRow extends AbstractCollection<Comparable> implements TableRow
{
  private final Comparable[] data;

  /**
   * Create new SimpleTableRow
   * @param data - items at this row
   * @param reqSize - size of 'data'. Should be same for all rows at same list
   * @param doCopy true if data should be copied or false if it's safety to use data without copy.
   */
  protected SimpleTableRow(Comparable[] data, int reqSize, boolean doCopy)
  {
    if (data.length != reqSize)
      throw new IllegalArgumentException("Expected " + reqSize + " columns, but there are " + data.length + " at " + this.getClass().getName());
    if (doCopy)
      this.data = Arrays.copyOf(data, data.length);
    else
      this.data = data;
  }

  /** Count of columns */
  @Override
  public int size()
  {
    return data.length;
  }

  /** Data by column index */
  @Override
  public Comparable getObject(int idx)
  {
    return data[idx];
  }

  /**
   * Text by column index. Override for custom formatting
   * @return obj.toString() for non-null objects, or null for null objects (not "null" string)
   */
  @Override
  public String getText(int idx)
  {
    Comparable obj = data[idx];
    return obj != null? obj.toString() : null;
  }

  /**
   * Utility method to format numbers. Add '0' prefix to stretch to 'minLength'
   * @return null for null objects (not "null" string)
   */
  protected String getText(int idx, int minLength)
  {
    Comparable obj = data[idx];
    return (obj != null)? fillToLen(obj.toString(), minLength) : null;
  }

  private String fillToLen(String text, int len)
  {
    len -= text.length();
    if (len <= 0)
      return text;
    switch (len)
    {
      case 1: return "0" + text;
      case 2: return "00" + text;
      case 3: return "000" + text;
      case 4: return "0000" + text;
      default:
        StringBuilder sb = new StringBuilder(text);
        while (--len >= 0)
          sb.insert(0, '0');
        return sb.toString();
    }
  }

  /** Returns unmodifiable array of all objects */
  @Override
  public Comparable[] getData()
  {
    return Arrays.copyOf(data, data.length);
  }

  /** Call this method at constructor to reduce memory consumption of duplicate values */
  @SuppressWarnings("unchecked")
  protected final void distinctColumns(int... columns)
  {
    for (int col: columns)
    {
      Comparable value = data[col];
      if (value != null)
      {
        Comparable canonical = internmap.putIfAbsent(value, value);
        if (canonical != null)
          data[col] = canonical;
      }
    }
  }

  private static final ConcurrentMap<Comparable, Comparable> internmap = new ConcurrentHashMap<>();

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(data);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null || getClass() != obj.getClass())
      return false;
    return Arrays.equals(this.data, ((SimpleTableRow) obj).data);
  }

  @Override
  public Iterator<Comparable> iterator()
  {
    return Collections.unmodifiableList(Arrays.asList(data)).iterator();
  }
}
