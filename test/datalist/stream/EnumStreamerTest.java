package datalist.stream;

import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import org.junit.Test;
import static org.junit.Assert.*;

public class EnumStreamerTest
{
  private static enum TestEnum {A, B, C, D, EE, FFF, G, H, I, K, L, M, n, o_o};

  @Test
  public void testOne() throws IOException
  {
    EnumStreamer<TestEnum> streamer = EnumStreamer.of(TestEnum.class);
    Path testFile = Files.createTempFile("test-", "-buff");
    testFile.toFile().deleteOnExit();
    try (FileChannel fc = FileChannel.open(testFile, CREATE, WRITE, READ))
    {
      try (WriteBuffer wb = new WriteBuffer(fc, 1000))
      {
        streamer.write(wb, TestEnum.FFF);
      }
      ReadBuffer rb = new ReadBuffer(fc, 10);
      assertEquals(TestEnum.FFF, streamer.read(rb));
    }
  }

  @Test
  public void testAll() throws IOException
  {
    EnumStreamer<TestEnum> streamer = EnumStreamer.of(TestEnum.class);
    Path testFile = Files.createTempFile("test-", "-buff");
    testFile.toFile().deleteOnExit();
    try (FileChannel fc = FileChannel.open(testFile, CREATE, WRITE, READ))
    {
      try (WriteBuffer wb = new WriteBuffer(fc, 1000))
      {
        for (TestEnum elem: TestEnum.values())
          streamer.write(wb, elem);
      }
      ReadBuffer rb = new ReadBuffer(fc, 10);
      for (TestEnum elem: TestEnum.values())
        assertEquals(elem, streamer.read(rb));
    }
  }
}
