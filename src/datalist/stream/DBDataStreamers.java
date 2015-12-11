package datalist.stream;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;
import java.util.Date;

/**
 * Collection of common streamers.<br>
 * Streamers *_with_nulls use one value to store null (that value not allowed), *_no_nulls stores all values, but null is prohibited.<br>
 * All streamers is thread-safe (without states)
 * @author SergeyT
 */
public class DBDataStreamers
{
  public static final DBDataStreamer<String> strings = new DBDataStreamer<String>()
  {
    @Override
    public String read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return buffer.getString();
    }

    @Override
    public void write(WriteBuffer buffer, String item)
    {
      buffer.putString(item);
    }

    @Override
    public String toString()
    {
      return "strings";
    }
  };

  public static final FixedSizeDataStreamer<Integer> ints_with_nulls = new FixedSizeDataStreamer<Integer>()
  {
    @Override
    public Integer read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      int ret = buffer.getInt();
      return ret == Integer.MIN_VALUE? null : ret;
    }

    @Override
    public void write(WriteBuffer buffer, Integer item)
    {
      if (item == null)
        buffer.putInt(Integer.MIN_VALUE);
      else if (item != Integer.MIN_VALUE)
        buffer.putInt(item);
      else
        throw new IllegalArgumentException("Minimal int value used as null");
    }

    @Override
    public int itemSize()
    {
      return 4;
    }

    @Override
    public String toString()
    {
      return "ints with nulls";
    }
  };

  public static final FixedSizeDataStreamer<Long> longs_with_nulls = new FixedSizeDataStreamer<Long>()
  {
    @Override
    public Long read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      long ret = buffer.getLong();
      return ret == Long.MIN_VALUE? null : ret;
    }

    @Override
    public void write(WriteBuffer buffer, Long item)
    {
      if (item == null)
        buffer.putLong(Long.MIN_VALUE);
      else if (item != Long.MIN_VALUE)
        buffer.putLong(item);
      else
        throw new IllegalArgumentException("Minimal long value used as null");
    }

    @Override
    public int itemSize()
    {
      return 8;
    }

    @Override
    public String toString()
    {
      return "longs with nulls";
    }
  };

  public static final FixedSizeDataStreamer<Byte> bytes_with_nulls = new FixedSizeDataStreamer<Byte>()
  {
    @Override
    public Byte read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      byte ret = buffer.get();
      return ret == Byte.MIN_VALUE? null : ret;
    }

    @Override
    public void write(WriteBuffer buffer, Byte item)
    {
      if (item == null)
        buffer.put(Byte.MIN_VALUE);
      else if (item != Byte.MIN_VALUE)
        buffer.put(item);
      else
        throw new IllegalArgumentException("Minimal byte value used as null");
    }

    @Override
    public int itemSize()
    {
      return 1;
    }

    @Override
    public String toString()
    {
      return "bytes with nulls";
    }
  };

  public static final FixedSizeDataStreamer<Double> doubles_with_nulls = new FixedSizeDataStreamer<Double>()
  {
    @Override
    public Double read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      final double ret = buffer.getDouble();
      if (Double.isNaN(ret))
        return null;
      else
        return ret;
    }

    @Override
    public void write(WriteBuffer buffer, Double item)
    {
      if (item == null)
        buffer.putDouble(Double.NaN);
      else if (!Double.isNaN(item))
        buffer.putDouble(item);
      else
        throw new IllegalArgumentException("NAN used as null");
    }

    @Override
    public int itemSize()
    {
      return 8;
    }

    @Override
    public String toString()
    {
      return "doubles with nulls";
    }
  };

  public static final FixedSizeDataStreamer<Integer> ints_no_nulls = new FixedSizeDataStreamer<Integer>()
  {
    @Override
    public Integer read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return buffer.getInt();
    }

    @Override
    public void write(WriteBuffer buffer, Integer item)
    {
      if (item == null)
        throw new IllegalArgumentException("Null not supported");
      buffer.putInt(item);
    }

    @Override
    public int itemSize()
    {
      return 4;
    }

    @Override
    public String toString()
    {
      return "ints not null";
    }
  };

  public static final FixedSizeDataStreamer<Long> longs_no_nulls = new FixedSizeDataStreamer<Long>()
  {
    @Override
    public Long read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return buffer.getLong();
    }

    @Override
    public void write(WriteBuffer buffer, Long item)
    {
      if (item == null)
        throw new IllegalArgumentException("Null not supported");
      buffer.putLong(item);
    }

    @Override
    public int itemSize()
    {
      return 8;
    }

    @Override
    public String toString()
    {
      return "longs not null";
    }
  };

  public static final FixedSizeDataStreamer<Byte> bytes_no_nulls = new FixedSizeDataStreamer<Byte>()
  {
    @Override
    public Byte read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return buffer.get();
    }

    @Override
    public void write(WriteBuffer buffer, Byte item)
    {
      if (item == null)
        throw new IllegalArgumentException("Null not supported");
      buffer.put(item);
    }

    @Override
    public int itemSize()
    {
      return 1;
    }

    @Override
    public String toString()
    {
      return "bytes not null";
    }
  };

  public static final FixedSizeDataStreamer<Double> doubles_no_nulls = new FixedSizeDataStreamer<Double>()
  {
    @Override
    public Double read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return buffer.getDouble();
    }

    @Override
    public void write(WriteBuffer buffer, Double item)
    {
      if (item == null)
        throw new IllegalArgumentException("Null not supported");
      buffer.putDouble(item);
    }

    @Override
    public int itemSize()
    {
      return 8;
    }

    @Override
    public String toString()
    {
      return "doubles not null";
    }
  };

  public static final FixedSizeDataStreamer<Date> dates = new FixedSizeDataStreamer<Date>()
  {
    @Override
    public Date read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return buffer.getDate();
    }

    @Override
    public void write(WriteBuffer buffer, Date item)
    {
      buffer.putDate(item);
    }

    @Override
    public int itemSize()
    {
      return 8;
    }

    @Override
    public String toString()
    {
      return "dates";
    }
  };

  private DBDataStreamers()
  {
  }
}
