package datalist.index;

import datalist.stream.DBDataStreamers;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.Assert;
import static org.junit.Assert.fail;
import org.junit.Test;

public class SetIndexTest
{
  @Test
  public void testSimple() throws IOException
  {
    final int keysCount = 100;
    final int itemsPerKey = 100;
    List<KeyToIndex<Integer>> map = createMap(keysCount, itemsPerKey);
    File idxFile = createTempFile();
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      index.recreate(map);
      checkResult(index, keysCount, itemsPerKey);
    }
    //close and reopen
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      checkResult(index, keysCount, itemsPerKey);
    }
  }

  @Test
  public void testRequestAll() throws IOException
  {
    final int keysCount = 10;
    final int itemsPerKey = 10;
    List<KeyToIndex<Integer>> map = createMap(keysCount, itemsPerKey);
    File idxFile = createTempFile();
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      index.recreate(map);
      Assert.assertEquals(keysCount * itemsPerKey, index.valuesFor(map.stream().map(x -> x.getKey()).collect(Collectors.toList())).size());
    }
    //close and reopen
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      Assert.assertEquals(keysCount * itemsPerKey, index.valuesFor(map.stream().map(x -> x.getKey()).collect(Collectors.toList())).size());
    }
  }

  @Test
  @SuppressWarnings("BoxingBoxedValue")
  public void testNulls() throws IOException
  {
    List<KeyToIndex<Double>> map = new ArrayList<>(4);
    map.add(new KeyToIndex<>(null, 0));
    map.add(new KeyToIndex<>(2.5, 1));
    map.add(new KeyToIndex<>(3.7, 2));
    map.add(new KeyToIndex<>(null, 3));
    File idxFile = createTempFile();
    try (SetIndex<Double> index = new SetIndex<>(idxFile, DBDataStreamers.doubles_with_nulls))
    {
      index.recreate(map);
      Assert.assertEquals(3, index.allKeys(true).size());
      Assert.assertEquals(2, index.allKeys(false).size());
      Assert.assertEquals(new SortedIntSet(0, 3), index.valuesFor((Double)null));
    }
    //close and reopen
    try (SetIndex<Double> index = new SetIndex<>(idxFile, DBDataStreamers.doubles_with_nulls))
    {
      Assert.assertEquals(3, index.allKeys(true).size());
      Assert.assertEquals(2, index.allKeys(false).size());
      Assert.assertEquals(new SortedIntSet(0, 3), index.valuesFor((Double)null));
    }
  }

  @Test
  public void testRecreateBigger() throws IOException
  {
    final int keysCount = 20;
    final int itemsPerKey = 20;
    File idxFile = createTempFile();
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      index.recreate(createMap(keysCount/2, itemsPerKey/2));
      checkResult(index, keysCount/2, itemsPerKey/2);
      index.recreate(createMap(keysCount, itemsPerKey)); //recreate bigger
      checkResult(index, keysCount, itemsPerKey);
    }
    //close and reopen
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      checkResult(index, keysCount, itemsPerKey);
    }
  }

  @Test
  public void testRecreateSmaller() throws IOException
  {
    final int keysCount = 20;
    final int itemsPerKey = 20;
    File idxFile = createTempFile();
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      index.recreate(createMap(keysCount*2, itemsPerKey*2));
      checkResult(index, keysCount*2, itemsPerKey*2);
      index.recreate(createMap(keysCount, itemsPerKey)); //recreate smaller
      checkResult(index, keysCount, itemsPerKey);
    }
    //close and reopen
    try (SetIndex<Integer> index = new SetIndex<>(idxFile, DBDataStreamers.ints_with_nulls))
    {
      checkResult(index, keysCount, itemsPerKey);
    }
  }

  private List<KeyToIndex<Integer>> createMap(final int keysCount, final int itemsPerKey)
  {
    List<KeyToIndex<Integer>> map = new ArrayList<>(keysCount * itemsPerKey);
    for (int i = 0; i < keysCount * itemsPerKey; ++i)
      map.add(new KeyToIndex<>(i % keysCount, i));
    return map;
  }

  private void checkResult(final SetIndex<Integer> index, final int keysCount, final int itemsPerKey) throws IOException
  {
    List<Integer> keys = index.allKeys(false); // false or true, nothing changes
    Assert.assertEquals(keysCount, keys.size());
    for (Integer key: keys)
    {
      SortedIntSet vals = index.valuesFor(key);
      Assert.assertEquals(itemsPerKey, vals.size());
      for (Integer val: vals)
        Assert.assertEquals((int)key, val % keysCount);
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
    final int keys = 200;
    final int total = 40000;
    final int threads = 12;

    if (total % keys != 0)
      throw new IllegalArgumentException("For code simplification, all keys should have same count of values");

    ArrayList<KeyToIndex<Integer>> data = new ArrayList<>(8);
    int idx = 0;
    for (int i = 0; i < total; ++i)
    {
      //Fill like [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4...]
      data.add(new KeyToIndex<>(idx++ % keys, i));
    }

    try (SortedIndex<Integer> index = new SortedIndex<>(createTempFile(), DBDataStreamers.ints_no_nulls))
    {
      index.recreate(data);
      CountDownLatch cdl = new CountDownLatch(1);
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      ArrayList<Future<String>> results = new ArrayList<>();
      for (int i = threads; --i >= 0;)
        results.add(pool.submit(new ConcurrentReadIndex(index, keys, total / keys, cdl, i * 17)));
      cdl.countDown();
      for (Future<String> res: results)
        if (res.get() != null)
          fail(res.get());
    }
  }

  private class ConcurrentReadIndex implements Callable<String>
  {
    private final Index<Integer> index;
    private final int keys_count;
    private final int duplicates;
    private final CountDownLatch cdl;
    private final Random rnd;

    private ConcurrentReadIndex(Index<Integer> index, int keys_count, int duplicates, CountDownLatch cdl, int seed)
    {
      this.index = index;
      this.keys_count = keys_count;
      this.duplicates = duplicates;
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
        int max = rnd.nextInt(keys_count);
        int min = rnd.nextInt(max + 1);
        SortedIntSet indexes = index.valuesFor(min, max);
        if (indexes.size() != (max - min + 1) * duplicates)
          return String.format("For range [%d, %d] found %d items", min, max, indexes.size());
        for (Integer idx: indexes)
        {
          int expVal = idx % keys_count;
          if (expVal < min || expVal > max)
            return String.format("Index %d contains %d, that not in range [%d, %d]", idx, expVal, min, max);
        }
      }
      return null;
    }
  }
}
