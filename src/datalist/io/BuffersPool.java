package datalist.io;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Keep buffers for each index, table etc requires too many memory. Frequent buffers allocation/gc is slow.<br>
 * BuffersPool keep few allocated buffers (10 or less by default), that used by all Indexes, text search.<br>
 * This class is thread-safe.
 * @author SergeyT
 */
public final class BuffersPool
{
  private static final int BUFFER_SIZE = Integer.getInteger("datalist.buffer_size", 2048);
  private static final ArrayBlockingQueue<ByteBuffer> abq = new ArrayBlockingQueue<>(10);
  //ArrayBlockingQueue allows to limit buffers count (in case of erroneous usage), but use lock.

  /** Get next buffer. Size specified at "datalist.buffer_size" system variable (2Mb by default). */
  public static ByteBuffer get()
  {
    ByteBuffer res = abq.poll();
    if (res != null)
    {
      res.clear();
      return res;
    }
    else
      return ByteBuffer.allocateDirect(BUFFER_SIZE);
  }

  /**
   * When buffer no more requires, it might be pushed to SharedBuffer list.<br>
   * Nothing happened if buffer has different size or buffers queue is full.<br>
   * <b>You should not use buffer after push it.</b>
   */
  public static void push(ByteBuffer buff)
  {
    if (buff != null && buff.capacity() == BUFFER_SIZE)
      abq.offer(buff);
  }

  private BuffersPool()
  {
  }
}
