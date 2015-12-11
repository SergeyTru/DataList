package datalist.list;

import datalist.io.ChannelBuilder;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * ItemOffsetList store offset for each item of {@link DataList}
 * @author SergeyT
 */
public class ItemOffsetList implements AutoCloseable
{
  private final FileChannel fc;
  private volatile long[] endings;

  public ItemOffsetList(Path path) throws IOException
  {
    fc = ChannelBuilder.forReadWrite(path).build();
    int cnt = (int)(fc.size() / 8);
    endings = new long[cnt];
    try (ReadBuffer rdr = new ReadBuffer(fc, -1))
    {
      for (int i = 0; i < cnt; ++i)
        endings[i] = rdr.getLong();
    }
  }

  public long from(int idx)
  {
    return idx == 0?0:endings[idx-1];
  }

  public long till(int idx)
  {
    return endings[idx];
  }

  public void clear() throws IOException
  {
    fc.truncate(0);
    endings = new long[0];
  }

  public void add(long[] items, int cnt)
  {
    long[] newEndings = Arrays.copyOf(endings, endings.length + cnt);
    System.arraycopy(items, 0, newEndings, endings.length, cnt);
    endings = newEndings;
    appendToFile(items, cnt);
  }

  private void appendToFile(long[] items, int cnt)
  {
    try (WriteBuffer wb = new WriteBuffer(fc, -1))
    {
      for (int i = 0; i < cnt; ++i)
        wb.putLong(items[i]);
    }
  }

  public int size()
  {
    return endings.length;
  }

  @Override
  public void close() throws IOException
  {
    fc.close();
  }
}