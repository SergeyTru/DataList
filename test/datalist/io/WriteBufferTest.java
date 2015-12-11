package datalist.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

public class WriteBufferTest
{
  @Test
  public void putLongBuff() throws IOException
  {
    byte[] buff = new byte[35];
    Random rnd = new Random(517); //fixed seed to reproduce test
    rnd.nextBytes(buff);

    FileChannel fc = ChannelBuilder.temporary("test-buff-", "").forRead().forWrite().deleteOnClose().build();
    try (WriteBuffer wb = new WriteBuffer(fc, 10))
    {
      wb.put(buff);
    }
    
    try (ReadBuffer rb = new ReadBuffer(fc, 10))
    {
      byte[] buff2 = new byte[buff.length];
      rb.read(buff2);
      assertArrayEquals(buff, buff2);
    }
  }
}
