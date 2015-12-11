
package datalist.stream;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DatabaseBrokenError;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * SerializationStreamer is DBDataStreamer, that serialize and deserialize objects using Java serialization mechanism<br><br>
 * SerializationStreamer is thread-safe.
 * @author SergeyT
 * @param <T> type of serialization objects ({@link ClassCastException} might be thrown on type mismatch)
 */
public class SerializationStreamer<T> implements DBDataStreamer<T>
{
  @Override
  @SuppressWarnings("unchecked")
  public T read(ReadBuffer buffer) throws DatabaseBrokenError
  {
    try (ObjectInputStream ois = new ObjectInputStream(new NotClosedInputStream(buffer)))
    {
      return (T)ois.readObject();
    }
    catch (ClassNotFoundException ex)
    {
      throw new IllegalStateException(ex);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public void write(WriteBuffer buffer, T item)
  {
    try (ObjectOutputStream oos = new ObjectOutputStream(new NotClosedOutputStream(buffer)))
    {
      oos.writeObject(item);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }
  
  private static class NotClosedInputStream extends InputStream
  {
    private final InputStream is;

    public NotClosedInputStream(InputStream is)
    {
      this.is = is;
    }

    @Override
    public int read() throws IOException
    {
      return is.read();
    }

    @Override
    public int read(byte[] b) throws IOException
    {
      return is.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
      return is.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException
    {
      return is.skip(n);
    }

    @Override
    public int available() throws IOException
    {
      return is.available();
    }

    @Override
    public void close() throws IOException
    {
      //do not is.close();
    }

    @Override
    public synchronized void mark(int readlimit)
    {
      is.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException
    {
      is.reset();
    }

    @Override
    public boolean markSupported()
    {
      return is.markSupported();
    }
  }
  
  private static class NotClosedOutputStream extends OutputStream
  {
    private final OutputStream os;

    public NotClosedOutputStream(OutputStream os)
    {
      this.os = os;
    }

    @Override
    public void write(int b) throws IOException
    {
      os.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
      os.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
      os.write(b, off, len);
    }

    @Override
    public void flush() throws IOException
    {
      os.flush();
    }

    @Override
    public void close() throws IOException
    {
      //do not os.close();
    }
  }
}
