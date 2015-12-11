package datalist.table;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;
import datalist.stream.DBDataStreamer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

public abstract class TableRowStreamer<T extends TableRow> implements DBDataStreamer<T>
{
  private final ArrayList<DBDataStreamer<? extends Comparable>> streamers;

  public TableRowStreamer(List<DBDataStreamer<? extends Comparable>> streamers)
  {
    this.streamers = new ArrayList<>(streamers);
  }

  @SuppressWarnings("unchecked")
  public TableRowStreamer(DBDataStreamer<? extends Comparable>... streamers)
  {
    this(Arrays.asList(streamers));
  }

  public DBDataStreamer<? extends Comparable> getStreamer(int index)
  {
    return streamers.get(index);
  }

  protected abstract T createObject(Comparable[] data);

  protected Comparable[] extractData(T item)
  {
    return item.getData();
  }

  @Override
  public T read(ReadBuffer buffer) throws DatabaseBrokenError
  {
    byte[] nullCheck = new byte[(streamers.size() + 7) / 8];
    buffer.get(nullCheck);
    BitSet nullsPositions = BitSet.valueOf(nullCheck);
    Comparable[] data = new Comparable[streamers.size()];
    int i = -1;
    try
    {
      for (DBDataStreamer<? extends Comparable> streamer: streamers)
        if (nullsPositions.get(++i))
          data[i] = null;
        else
          data[i] = streamer.read(buffer);
    }
    catch (ClassCastException ex)
    {
      String vl = data[i] == null? data[i].toString() + " (" + data[i].getClass().toString() + ')' : " null";
      throw new IllegalStateException("Streamer " + streamers.get(i) + " fail to read " + vl + ", column " + i, ex);
    }
    if (buffer.getInt() != Arrays.hashCode(data))
      throw new DatabaseBrokenError("Hash code invalid");
    return createObject(data);
  }

  @Override
  public void write(WriteBuffer buffer, T item)
  {
    Comparable[] data = extractData(item);
    if (data.length != streamers.size())
      throw new IllegalArgumentException();
    int i = data.length;
    try
    {
      BitSet nullsPositions = new BitSet(data.length);
      while (--i >= 0)
        nullsPositions.set(i, data[i] == null);
      final byte[] retArray = nullsPositions.toByteArray();
      buffer.put(retArray);
      i = (streamers.size() + 7) / 8 - retArray.length;
      while (--i >= 0)
        buffer.put((byte)0);
      // int i = -1; //already -1
      for (DBDataStreamer<? extends Comparable> streamer: streamers)
        if (!nullsPositions.get(++i))
          streamer.writeGeneric(buffer, data[i]);
    }
    catch (ClassCastException ex)
    {
      String vl = data[i] == null? data[i].toString() + " (" + data[i].getClass().toString() + ')' : " null";
      throw new IllegalStateException("Streamer " + streamers.get(i) + " fail to write " + vl + ", column " + i, ex);
    }
    buffer.putInt(Arrays.hashCode(data));
  }

  @Override
  public int hashCode()
  {
    return streamers.size();
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj != null && getClass() == obj.getClass() && Objects.equals(this.streamers, ((TableRowStreamer) obj).streamers);
  }
}