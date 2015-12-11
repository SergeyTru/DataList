package datalist.index;

import datalist.stream.DBDataStreamers;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

public class SortedIndexTest
{
  @Test
  public void testGeneral() throws IOException
  {
    try (SortedIndex<Integer> index = new SortedIndex<>(createTempFile(), DBDataStreamers.ints_with_nulls))
    {
      for (int offset = 0; offset < 5; ++offset)
      {
        ArrayList<KeyToIndex<Integer>> data = new ArrayList<>(20);
        for (int arraySize = 1; arraySize < 20; ++arraySize)
        {
          data.add(new KeyToIndex<>(arraySize - 1 + offset, arraySize - 1));
          index.recreate(data);
          for (int i = 0; i < arraySize; ++i)
            for (int j = i; j < arraySize; ++j)
            {
              SortedIntSet vals = index.valuesFor(i + offset, j + offset);
              if (vals.size() != j - i + 1) //for easy debug
              {
                vals = index.valuesFor(i + offset, j + offset);
                assertEquals(String.format("size=%d, offs=%d, i=%d, j=%d", arraySize, offset, i, j), j - i + 1, vals.size());
              }
              int exp = i - 1;
              for (Integer val : vals)
                if (++exp != val) //for easy debug
                  assertEquals(exp, (int) val);
            }
          assertEquals(offset, (int) index.min());
          assertEquals(arraySize - 1 + offset, (int) index.max());
        }
      }
    }
  }

  @Test
  public void testDoubles() throws IOException
  {
    File idxFile = createTempFile();
    ArrayList<KeyToIndex<Double>> data = new ArrayList<>(8);
    data.add(new KeyToIndex<>(-1., 6));
    data.add(new KeyToIndex<>(7.1, 5));
    data.add(new KeyToIndex<>(3.2, 4));
    data.add(new KeyToIndex<>(4.3, 3));
    data.add(new KeyToIndex<>(2.4, 2));
    data.add(new KeyToIndex<>(3.2, 1));
    data.add(new KeyToIndex<>(-7., 6));
    try (SortedIndex<Double> index = new SortedIndex<>(idxFile, DBDataStreamers.doubles_with_nulls))
    {
      index.recreate(data);
      doTestDoubles(index);
    }
    try (SortedIndex<Double> index = new SortedIndex<>(idxFile, DBDataStreamers.doubles_with_nulls))
    {
      doTestDoubles(index);
    }
  }

  private void doTestDoubles(final SortedIndex<Double> index) throws IOException
  {
    assertEquals(-7., index.min(), 0.001);
    assertEquals(7.1, index.max(), 0.001);

    //test for sorting indexes
    SortedIntSet rng = index.valuesFor(3.2, 3.2);
    assertEquals(2, rng.size());
    assertEquals(1, (int) rng.get(0));
    assertEquals(4, (int) rng.get(1));

    //test for non-exact
    rng = index.valuesFor(-2., 3.);
    assertEquals(2, rng.size());
    assertEquals(2, (int) rng.get(0));
    assertEquals(6, (int) rng.get(1));

    //test left bound
    rng = index.valuesFor(-8., -6.9);
    assertEquals(1, rng.size());
    assertEquals(6, (int) rng.get(0));

    //test right bound
    rng = index.valuesFor(7., 8.);
    assertEquals(1, rng.size());
    assertEquals(5, (int) rng.get(0));

    //test outside left bound
    rng = index.valuesFor(-9., -8.);
    assertEquals(0, rng.size());

    //test outside right bound
    rng = index.valuesFor(8., 9.);
    assertEquals(0, rng.size());

    //test inside empty
    rng = index.valuesFor(1.1, 1.2);
    assertEquals(0, rng.size());
  }

  @Test
  @SuppressWarnings("BoxingBoxedValue")
  public void testNulls() throws IOException
  {
    File idxFile = createTempFile();
    ArrayList<KeyToIndex<Double>> data = new ArrayList<>(8);
    data.add(new KeyToIndex<>(null, 0));
    data.add(new KeyToIndex<>(2.5, 1));
    data.add(new KeyToIndex<>(3.7, 2));
    data.add(new KeyToIndex<>(null, 3));

    try (SortedIndex<Double> index = new SortedIndex<>(idxFile, DBDataStreamers.doubles_with_nulls))
    {
      index.recreate(data);
      assertEquals(3, index.allKeys(true).size());
      assertEquals(2, index.allKeys(false).size());
      assertEquals(2.5, index.min(), 0.01);
      assertEquals(3.7, index.max(), 0.01);
      assertEquals(new SortedIntSet(0, 3), index.valuesFor((Double) null));
    }
    //close and reopen
    try (SortedIndex<Double> index = new SortedIndex<>(idxFile, DBDataStreamers.doubles_with_nulls))
    {
      assertEquals(3, index.allKeys(true).size());
      assertEquals(2, index.allKeys(false).size());
      assertEquals(2.5, index.min(), 0.01);
      assertEquals(3.7, index.max(), 0.01);
      assertEquals(new SortedIntSet(0, 3), index.valuesFor((Double) null));
    }
  }

  private File createTempFile() throws IOException
  {
    File idxFile = Files.createTempFile("test-index-", "").toFile();
    idxFile.deleteOnExit();
    return idxFile;
  }

  @Test
  public void testConcurrent() throws IOException, InterruptedException, ExecutionException
  {
    final int middle = 40000;
    final int threads = 12;

    ArrayList<KeyToIndex<Integer>> data = new ArrayList<>(8);
    int idx = 0;
    for (int i = 0; i < middle; ++i)
    {
      //Fill like [0, 6, 1, 7, 2, 8, 3, 9, 4, 10, 5, 11]
      data.add(new KeyToIndex<>(i, idx++));
      data.add(new KeyToIndex<>(i + middle, idx++));
    }

    File idxFile = createTempFile();
    try (SortedIndex<Integer> index = new SortedIndex<>(idxFile, DBDataStreamers.ints_no_nulls))
    {
      index.recreate(data);
      CountDownLatch cdl = new CountDownLatch(1);
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      ArrayList<Future<String>> results = new ArrayList<>();
      for (int i = threads; --i >= 0;)
        results.add(pool.submit(new ConcurrentReadIndex(index, middle, cdl, i * 17)));
      cdl.countDown();
      for (Future<String> res: results)
        if (res.get() != null)
          fail(res.get());
    }
  }

  private class ConcurrentReadIndex implements Callable<String>
  {
    private final Index<Integer> index;
    private final int middle;
    private final CountDownLatch cdl;
    private final Random rnd;

    private ConcurrentReadIndex(Index<Integer> index, int middle, CountDownLatch cdl, int seed)
    {
      this.index = index;
      this.middle = middle;
      this.cdl = cdl;
      this.rnd = new Random(seed); //fixed seed to reproduce
    }

    @Override
    public String call() throws InterruptedException, IOException
    {
      cdl.await();
      int checks = 10000;
      while (--checks >= 0)
      {
        int max = rnd.nextInt(middle * 2);
        int min = rnd.nextInt(max + 1);
        SortedIntSet indexes = index.valuesFor(min, max);
        if (indexes.size() != max - min + 1)
          return String.format("For range [%d, %d] found %d items", min, max, indexes.size());
        for (Integer idx : indexes)
        {
          int expVal = (idx & 1) == 1 ? idx / 2 + middle : idx / 2;
          if (expVal < min || expVal > max)
            return String.format("Index %d contains %d, that not in range [%d, %d]", idx, expVal, min, max);
        }
      }
      return null;
    }
  }
}
