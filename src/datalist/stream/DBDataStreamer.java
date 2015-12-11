package datalist.stream;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;

/**
 * DBDataStreamer used by DataList to read objects from file and write it.
 * {@link datalist.list.DataList} is thread-safe only when DBDataStreamer is thread-safe.
 */
public interface DBDataStreamer<T>
{
  /** read item from stream */
  T read(ReadBuffer buffer) throws DatabaseBrokenError;

  /** write item to stream */
  void write(WriteBuffer buffer, T item);

  /** Do not override this method. It used to avoid class cast error */
  @SuppressWarnings("unchecked")
  default void writeGeneric(WriteBuffer buffer, Comparable item)
  {
    write(buffer, (T)item);
  }
}
