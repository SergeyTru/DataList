package datalist.stream;

/**
 * Any FixedSizeDatabaseStreamer should read and write exactly itemSize() bytes.
 * It allows to calculate item position by index.
 */
public interface FixedSizeDataStreamer<T> extends DBDataStreamer<T>
{
  int itemSize();
}
