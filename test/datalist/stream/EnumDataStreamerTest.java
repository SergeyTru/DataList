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
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class EnumDataStreamerTest
{
  @DataPoints
  public static Integer[] dataSizes()
  {
    return new Integer[] {10, 0xFC, 0xFD, 0xFE, 0xFF, 0x100, 0xFFFC, 0xFFFD, 0xFFFE, 0xFFFF, 0x10000, 0x10001};
  }

  @Theory
  public void testReadWrite(Integer dataSize) throws IOException
  {
    Path testFile = Files.createTempFile("test-", "-buff");
    testFile.toFile().deleteOnExit();
    List<String> data = generateData(dataSize);
    try (FileChannel fc = FileChannel.open(testFile, CREATE, WRITE, READ))
    {
      EnumDataStreamer eds = new EnumDataStreamer(data);
      data.add(2, null);
      data.add(dataSize - 2, DUMMY);
      try (WriteBuffer wb = new WriteBuffer(fc, 1000))
      {
        for (String str: data)
          eds.write(wb, str);
      }
      ReadBuffer rb = new ReadBuffer(fc, 10);
      for (String str: data)
        assertEquals(str, eds.read(rb));
    }
  }

  private static final String DUMMY = "dummy";
  private static final int STEP = 137; //mix data to check if index used instead of data.

  private List<String> generateData(int size)
  {
    if (gcd(DUMMY.length(), STEP) != 1)
      throw new IllegalArgumentException("STEP and array size should be coprime!!!");

    List<String> result = new ArrayList<>(size + 2); //2 items added later
    int val = 0;
    while (result.size() < size)
    {
      val = (val + STEP) % size;
      result.add(String.valueOf(val));
    }
    return result;
  }

  // http://stackoverflow.com/questions/28575416/finding-out-if-two-numbers-are-relatively-prime
  private static int gcd(int a, int b)
  {
    int t;
    while(b != 0)
    {
      t = a;
      a = b;
      b = t%b;
    }
    return a;
  }
}
