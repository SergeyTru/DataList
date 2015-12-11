package datalist.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ReadBufferTest
{
  private static final String SUCCESS = "Success";

  @Test
  public void testSkipStringInside() throws IOException
  {
    try (FileChannel fc = prepareTestData())
    {
      ReadBuffer rb = new ReadBuffer(fc, 100);
      assertEquals(1, rb.getInt());
      rb.skipString();
      assertEquals(2, rb.getInt());
      assertEquals(SUCCESS, rb.getString());
    }
  }

  @Test
  public void testSkipStringOutside() throws IOException
  {
    try (FileChannel fc = prepareTestData())
    {
      ReadBuffer rb = new ReadBuffer(fc, 10);
      assertEquals(1, rb.getInt());
      rb.skipString();
      assertEquals(2, rb.getInt());
      assertEquals(SUCCESS, rb.getString());
    }
  }

  private FileChannel prepareTestData() throws IOException
  {
    Path testFile = Files.createTempFile("test-", "-buff");
    testFile.toFile().deleteOnExit();
    FileChannel fc = FileChannel.open(testFile, CREATE, WRITE, READ);
    try (WriteBuffer wb = new WriteBuffer(fc, 1000))
    {
      wb.putInt(1);
      wb.putString("This is test String");
      wb.putInt(2);
      wb.putString(SUCCESS);
    }
    return fc;
  }
}