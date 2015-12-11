package datalist.io;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Helping class to open FileChannel
 * @author SergeyT
 */
public class ChannelBuilder
{
  private final Path path;
  private final Collection<OpenOption> options = new ArrayList<>();
  private Boolean allowCreate;

  public ChannelBuilder(Path path)
  {
    this.path = path;
  }

  public ChannelBuilder(File file)
  {
    this.path = file.toPath();
  }

  /** Equals to new ChannelBuilder(file).forRead() */
  public static ChannelBuilder forRead(File file)
  {
    return new ChannelBuilder(file).forRead();
  }

  /** Equals to new ChannelBuilder(file).forWrite() */
  public static ChannelBuilder forWrite(File file)
  {
    return new ChannelBuilder(file).forWrite();
  }

  /** Equals to new ChannelBuilder(file).forRead().forWrite() */
  public static ChannelBuilder forReadWrite(File file)
  {
    return new ChannelBuilder(file).forRead().forWrite();
  }

  public static ChannelBuilder forReadWrite(Path path)
  {
    return new ChannelBuilder(path).forRead().forWrite();
  }

  /** By default file creates for write and does not creates for read */
  public ChannelBuilder allowCreate(boolean allow)
  {
    allowCreate = allow;
    if (allow)
      options.add(StandardOpenOption.CREATE);
    else
      options.remove(StandardOpenOption.CREATE);
    return this;
  }

  /**
   * Creates work file at temporary directory. This file will be deleted on close.
   * @param prefix is prefix of temporary file
   * @param suffix is suffix of temporary file
   * @throws IOException if temporary file could not be created
   */
  public static ChannelBuilder temporary(String prefix, String suffix) throws IOException
  {
    return new ChannelBuilder(Files.createTempFile(prefix, suffix)).deleteOnClose();
  }

  /** File should be readable */
  public ChannelBuilder forRead()
  {
    options.add(StandardOpenOption.READ);
    if (allowCreate == Boolean.TRUE)
      options.add(StandardOpenOption.CREATE);
    return this;
  }

  /** File should be writeble */
  public ChannelBuilder forWrite()
  {
    options.add(StandardOpenOption.WRITE);
    if (allowCreate != Boolean.FALSE)
      options.add(StandardOpenOption.CREATE);
    return this;
  }

  /** File should be deleted on close */
  public ChannelBuilder deleteOnClose()
  {
    options.add(StandardOpenOption.DELETE_ON_CLOSE);
    return this;
  }

  /** In case when you need specific options of FileChannel.open(), call this method */
  public ChannelBuilder withOptions(OpenOption... options)
  {
    this.options.addAll(Arrays.asList(options));
    return this;
  }

  /** Open file channel and return it */
  public FileChannel build() throws IOException
  {
    return FileChannel.open(path, options.toArray(new OpenOption[options.size()]));
  }

  /** Create new reader for constructed channel. Channel will be closed when reader closed.<br>
      Useful for read whole file.*/
  public ReadBuffer reader() throws IOException
  {
    final FileChannel fc = forRead().build();
    return new ReadBuffer(fc, -1)
    {
      @Override
      public void close()
      {
        try
        {
          super.close();
          fc.close();
        }
        catch (IOException ex)
        {
          throw new UncheckedIOException(ex);
        }
      }
    };
  }

  /** Create new writer for constructed channel. Channel will be closed when reader closed */
  public WriteBuffer writer() throws IOException
  {
    final FileChannel fc = forWrite().build();
    return new WriteBuffer(fc, -1)
    {
      @Override
      public void close()
      {
        try
        {
          super.close();
          fc.close();
        }
        catch (IOException ex)
        {
          throw new UncheckedIOException(ex);
        }
      }
    };
  }

  /** Create new writer for constructed channel. Channel will be closed when reader closed.
   @param capacity buffer capacity. To reduce IO operations count, use bigger buffer.*/
  public WriteBuffer writer(int capacity) throws IOException
  {
    final FileChannel fc = forWrite().build();
    return new WriteBuffer(fc, capacity)
    {
      @Override
      public void close()
      {
        try
        {
          super.close();
          fc.close();
        }
        catch (IOException ex)
        {
          throw new UncheckedIOException(ex);
        }
      }
    };
  }
}