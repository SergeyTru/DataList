package datalist.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * <p>Read-only wrapper for ByteBuffer, that automatically read data from FileChannel.</p>
 * <p>Does not close FileChannel. You should close it manually.</p>
 * <p>Multithread application should create ReadBuffer for each thread of synchronize all methods.</p>
 * <p>If storage fails, UncheckedIOException will be thrown.
 * @author SergeyT
 */
public class ReadBuffer extends InputStream implements AutoCloseable
{
  private final FileChannel fc;
  private ByteBuffer buff;
  private long pos;

  /**
   * Create new ReadBuffer for fc.
   * @param fc file channel to read
   * @param capacity size of buffer or -1 to use shared buffers
   * @see BuffersPool
   */
  public ReadBuffer(FileChannel fc, int capacity)
  {
    if (capacity <= 0 && capacity != -1)
      throw new IllegalArgumentException("Wrong capacity " + capacity);
    this.fc = fc;
    if (capacity == -1)
      buff = BuffersPool.get();
    else
      this.buff = ByteBuffer.allocate(capacity);
    buff.limit(0); //read by first request
  }

  /** Notify read buffer that data should be rereaded */
  public void invalidateBuffer()
  {
    buff.limit(0); //read by first request
  }

  /** Release buffer, but does not close FileChannel */
  @Override
  public void close()
  {
    BuffersPool.push(buff);
    buff = null;
  }

  /** Returns position in file */
  public final long position()
  {
    return pos + buff.position();
  }

  /**
   * Set position in file.<br>
   * Implementation details: modifies a variable, but does not load data till first reading
   */
  public ReadBuffer position(long newPos)
  {
    if (newPos > pos && newPos < pos + buff.limit())
    {
      buff.position((int) (newPos - pos));
      return this;
    }
    pos = newPos;
    buff.limit(0);
    return this;
  }

  /** Tells whether there are any elements between the current position and the end of file */
  public boolean hasRemaining()
  {
    try
    {
      return buff.hasRemaining() || pos + buff.position() < fc.size();
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  /** Tells count of bytes between the current position and the end of file */
  public long remaining()
  {
    try
    {
      return fc.size() - pos - buff.position();
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  public byte get()
  {
    checkGet(1);
    return buff.get();
  }

  /** Read data to 'dst' array. Buffer size should be bigger than 'dst' array */
  public void get(byte[] dst)
  {
    int dstPos = 0;
    int dstLen = dst.length;
    while (dstPos < dstLen)
    {
      int cnt = dstLen - dstPos;
      checkGet(cnt);

      cnt = Math.min(cnt, buff.remaining());
      buff.get(dst, dstPos, cnt);
      dstPos += cnt;
    }
  }

  public char getChar()
  {
    checkGet(2);
    return buff.getChar();
  }

  public byte getByte()
  {
    checkGet(1);
    return buff.get();
  }

  public short getShort()
  {
    checkGet(2);
    return buff.getShort();
  }

  public int getInt()
  {
    checkGet(4);
    return buff.getInt();
  }

  public long getLong()
  {
    checkGet(8);
    return buff.getLong();
  }

  public double getDouble()
  {
    checkGet(8);
    return buff.getDouble();
  }

  /** Load text, that stored by {@link WriteBuffer#putString(java.lang.String) WriteBuffer.putString(String)}*/
  public String getString()
  {
    int size = getInt();
    if (size == -1)
      return null;

    checkGet(size);
    byte[] bytes = new byte[size];
    buff.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /** Skip string without read it to memory */
  public void skipString()
  {
    int size = getInt();
    if (size > 0)
      position(position() + size);
  }

  /**
   * Read {@link java.util.Date}.<br>
   * Implementation details: timestamp='0' used for null date
   */
  public Date getDate()
  {
    long val = getLong();
    if (val != 0)
      return new Date(val);
    else
      return null;
  }

  /** Before each read, check if buffer contains enought data */
  private void checkGet(int size)
  {
    if (buff.remaining() >= size)
      return;

    pos += buff.position();
    buff.compact();

    try
    {
      while (buff.hasRemaining() && buff.position() < size && fc.position() < fc.size())
        fc.read(buff, pos + buff.position());
      buff.flip();
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public int read()
  {
    if (hasRemaining())
      return getByte();
    else
      return -1;
  }

  @Override
  public int read(byte[] b)
  {
    long old = position();
    get(b);
    return Math.toIntExact(pos - old);
  }

  @Override
  public int available()
  {
    return Math.toIntExact(remaining());
  }

  @Override
  public long skip(long n)
  {
    long old = position();
    position(old + n);
    return pos - old;
  }
}
