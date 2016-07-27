package datalist.stream;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;

/**
 * Streamer for enum fields.<br><br>
 * <strong>You could not change enum (add or remove values) after writing data.</strong><br><br>
 * EnumStreamer is thread-safe.
 */
public class EnumStreamer<T extends Enum<T>> implements DBDataStreamer<T>
{
  public static <T extends Enum<T>> EnumStreamer<T> of(Class<T> elementType)
  {
    return new EnumStreamer<>(elementType);
  }

  private final T[] universe;

  private EnumStreamer(Class<T> elementType)
  {
    universe = elementType.getEnumConstants();
  }
  
  @Override
  public T read(ReadBuffer buffer) throws DatabaseBrokenError
  {
    if (universe.length < 0xFF)
    {
      byte pos = buffer.getByte();
      if (pos == -1)
        return null;
      return universe[pos & 0xFF]; //unsigned
    }
    else if (universe.length < 0xFFFF)
    {
      short pos = buffer.getShort();
      if (pos == -1)
        return null;
      return universe[pos & 0xFFFF]; //unsigned
    }
    else
    {
      int pos = buffer.getInt();
      if (pos == -1)
        return null;
      return universe[pos]; //java collections limit is signed int
    }
  }

  @Override
  public void write(WriteBuffer buffer, T item)
  {
    int val = item==null?-1:item.ordinal();

    if (universe.length < 0xFF)
      buffer.put((byte)val);
    else if (universe.length < 0xFFFF)
      buffer.putShort((short)val);
    else
      buffer.putInt(val);
  }
}
