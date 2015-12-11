package datalist.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * <p>Write-only wrapper for ByteBuffer which automatically flush data to FileChannel.</p>
 * <p>Does not close FileChannel. You should close it manually.</p>
 * <p>Multithread application should create WriteBuffer for each thread or synchronize all methods.</p>
 * <p>If storage fails, UncheckedIOException will be thrown.
 * @author SergeyT
 */
public class WriteBuffer extends OutputStream implements AutoCloseable
{
  private final FileChannel fc;
  private ByteBuffer buff;
  private long pos = -1;

  /**
   * Create new WriteBuffer for fc.
   * @param fc file channel for write
   * @param capacity size of buffer or -1 to use shared buffers
   * @see BuffersPool
   */
  public WriteBuffer(FileChannel fc, int capacity)
  {
    if (capacity <= 0 && capacity != -1)
      throw new IllegalArgumentException("Wrong capacity " + capacity);
    this.fc = fc;
    if (capacity == -1)
      this.buff = BuffersPool.get();
    else
      this.buff = ByteBuffer.allocate(capacity);
  }

  public void put(byte b)
  {
    checkPut(1);
    buff.put(b);
  }

  /** Put data from 'src' array to buffer (and then to file). Buffer size should be bigger than 'src' array */
  public void put(byte[] src)
  {
    int srcPos = 0;
    int srcLen = src.length;
    if (buff.remaining() > srcLen)
    {
      buff.put(src);
      return;
    }
    while (srcPos < srcLen)
    {
      int rem = buff.remaining();
      if (rem < 16) //write at least 16 bytes
      {
        flush();
        rem = buff.remaining();
      }
      int cnt = Math.min(srcLen - srcPos, rem);
      buff.put(src, srcPos, cnt);
      srcPos += cnt;
    }
  }

  public void putChar(char value)
  {
    checkPut(2);
    buff.putChar(value);
  }

  public void putShort(short value)
  {
    checkPut(2);
    buff.putShort(value);
  }

  public void putInt(int value)
  {
    checkPut(4);
    buff.putInt(value);
  }

  public void putLong(long value)
  {
    checkPut(8);
    buff.putLong(value);
  }

  public void putDouble(double value)
  {
    checkPut(8);
    buff.putDouble(value);
  }

  /** Put text to file. Use {@link ReadBuffer#getString() ReadBuffer.getString()} to read it */
  public void putString(String text)
  {
    if (text == null)
    {
      putInt(-1);
      return;
    }
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    checkPut(4 + bytes.length);
    buff.putInt(bytes.length);
    buff.put(bytes);
  }

  /**
   * Store {@link java.util.Date}.<br>
   * Implementation details: timestamp='0' used for null date
   */
  public void putDate(Date date)
  {
    if (date != null)
      putLong(date.getTime());
    else
      putLong(0);
  }

  /** Put all buffered data to file (if any) and cleanup buffer */
  @Override
  public final void flush()
  {
    try
    {
      if (buff.position() == 0)
        return;
      if (pos < 0)
        pos = fc.size();
      buff.flip();
      while (buff.hasRemaining())
        fc.write(buff, pos);
      pos += buff.position();
      buff.clear();
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  /** Returns position in file */
  public final long position()
  {
    try
    {
      if (pos < 0)
        pos = fc.size();
      return pos + buff.position();
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  /**
   * Set position in file.<br>
   */
  public final void position(long newPosition)
  {
    try
    {
      flush();
      pos = newPosition == -1 ? fc.size() : newPosition;
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  /** Cut file to current position. */
  public void truncateTail()
  {
    try
    {
      flush();
      fc.truncate(pos);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  /** Before each write, check if buffer have enought space */
  private void checkPut(int size)
  {
    if (buff.remaining() < size)
      flush();
  }

  /** Flush data, release buffer, but does not close FileChannel */
  @Override
  public void close()
  {
    if (buff.position() > 0)
      flush();
    BuffersPool.push(buff);
    buff = null;
  }

  /**
   * Copy all data from other channel. FromChannel should be open for read too.
   */
  public boolean copyAllFrom(FileChannel fromChannel)
  {
//    //Fast, but insecure way (position is state)
//    flush();
//    long size = fc.size();
//    fc.position(pos);
//    return fc.transferFrom(fromChannel, 0, size) == size;
    try
    {
      checkPut(1024); //do not read few bytes, ensure that buffer is big
      long readPos = 0;
      long size = fc.size();
      while (readPos < size)
      {
        int read = fromChannel.read(buff, readPos);
        if (read < 0)
          return false;
        readPos += read;
        if (!buff.hasRemaining())
          flush();
      }
      return true;
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public void write(int b)
  {
    put((byte)b);
  }

  @Override
  public void write(byte[] b)
  {
    put(b);
  }
}
