package datalist.stream;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Streamer for enum-like text columns. Write index for values in list and whole text for other.<br><br>
 * <strong>You should use same values for read and write.</strong><br><br>
 * <strong>Chars case lost for predefined values.</strong><br><br>
 * EnumDataStreamer is thread-safe.
 */
public class EnumDataStreamer implements DBDataStreamer<String>
{
  private final List<String> values;
  private final HashMap<String, Integer> map;

  /**
   * Creates compact streamer for values. Handles nulls and other values correctly.
   * @param values - predefined set of most often values, Should not be null or contains nulls or duplicates
   */
  public EnumDataStreamer(Collection<String> values)
  {
    this.values = new ArrayList<>(values);
    this.map = new HashMap<>(values.size());
    int i = this.values.size();
    while (--i >= 0)
      if (map.put(this.values.get(i).toLowerCase(), i) != null)
        throw new IllegalArgumentException("Duplicate of " + this.values.get(i));
  }

  @Override
  public String read(ReadBuffer buffer) throws DatabaseBrokenError
  {
    if (values.size() <= 0xFD)
    {
      byte pos = buffer.getByte();
      if (pos == -1)
        return null;
      if (pos == -2)
        return buffer.getString();
      return values.get(pos & 0xFF); //unsigned
    }
    else if (values.size() <= 0xFFFD)
    {
      short pos = buffer.getShort();
      if (pos == -1)
        return null;
      if (pos == -2)
        return buffer.getString();
      return values.get(pos & 0xFFFF); //unsigned
    }
    else
    {
      int pos = buffer.getInt();
      if (pos == -1)
        return null;
      if (pos == -2)
        return buffer.getString();
      return values.get(pos); //java collections limit is signed int
    }
  }

  @Override
  public void write(WriteBuffer buffer, String item)
  {
    int val;
    if (item != null)
    {
      Integer pos = map.get(item.toLowerCase());
      val = (pos != null)? pos : -2;
    }
    else
      val = -1;

    if (values.size() <= 0xFD)
      buffer.put((byte)val);
    else if (values.size() <= 0xFFFD)
      buffer.putShort((short)val);
    else
      buffer.putInt(val);

    if (val == -2)
      buffer.putString(item);
  }
}