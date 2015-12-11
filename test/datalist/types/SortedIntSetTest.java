package datalist.types;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

public class SortedIntSetTest
{
  @Test
  public void testIntersect()
  {
    SortedIntSet res = new SortedIntSet(1, 3, 7);
    res.intersect(new SortedIntSet(3, 5, 7));
    assertEquals(2, res.size());
    assertEquals(3, res.get(0));
    assertEquals(7, res.get(1));
    res.intersect(new SortedIntSet(7, 3));
    assertEquals(2, res.size());
    assertEquals(3, res.get(0));
    assertEquals(7, res.get(1));
    res.intersect(new SortedIntSet(5, 7));
    assertEquals(1, res.size());
    assertEquals(7, res.get(0));
  }

  @Test
  public void testMultiIntersect()
  {
    SortedIntSet res = new SortedIntSet(7, 3, 9, 1);
    res.intersect(new SortedIntSet(3, 5, 7, 9));
    assertEquals(3, res.size());
    assertEquals(3, res.get(0));
    assertEquals(7, res.get(1));
    assertEquals(9, res.get(2));
    res.intersect(new SortedIntSet(3, 7, 8, 9, 11));
    assertEquals(3, res.size());
    assertEquals(3, res.get(0));
    assertEquals(7, res.get(1));
    assertEquals(9, res.get(2));
    res.intersect(new SortedIntSet(5, 7, 8, 9, 13));
    assertEquals(2, res.size());
    assertEquals(7, res.get(0));
    assertEquals(9, res.get(1));
  }

  @Test
  public void testEmptyIntersect()
  {
    SortedIntSet res = new SortedIntSet();
    res.intersect(new SortedIntSet());
    assertEquals(0, res.size()); //actually, check for errors

    res = new SortedIntSet(1, 2);
    res.intersect(new SortedIntSet());
    assertEquals(0, res.size());

    res = new SortedIntSet();
    res.intersect(new SortedIntSet(1, 2));
    assertEquals(0, res.size());
  }

  @Test
  public void testAppend()
  {
    SortedIntSet res = new SortedIntSet(1, 3, 7);
    res.union(new SortedIntSet(1, 2, 3));
    assertEquals(4, res.size());
    assertEquals(1, res.get(0));
    assertEquals(2, res.get(1));
    assertEquals(3, res.get(2));
    assertEquals(7, res.get(3));
    res.union(new SortedIntSet(new int[]{5}));
    assertEquals(5, res.size());
    assertEquals(1, res.get(0));
    assertEquals(2, res.get(1));
    assertEquals(3, res.get(2));
    assertEquals(5, res.get(3));
    assertEquals(7, res.get(4));
    res.union(new SortedIntSet());
    assertEquals(5, res.size());
    assertEquals(1, res.get(0));
    assertEquals(2, res.get(1));
    assertEquals(3, res.get(2));
    assertEquals(5, res.get(3));
    assertEquals(7, res.get(4));
  }

  @Test
  public void testSpecialIntersect()
  {
    SortedIntSet any = new SortedIntSet(1, 3, 7);
    SortedIntSet empty = SortedIntSet.empty();
    SortedIntSet full = SortedIntSet.allValues(100);

    any.intersect(full);
    assertEquals(3, any.size());

    any.intersect(empty);
    assertEquals(0, any.size());
  }

  @Test
  public void testSpecialAppend()
  {
    SortedIntSet any = new SortedIntSet(1, 3, 7);
    SortedIntSet empty = SortedIntSet.empty();
    SortedIntSet full = SortedIntSet.allValues(100);

    any.union(empty);
    assertEquals(3, any.size());

    any.union(full);
    assertTrue(any.allValuesList());
  }

  @Test
  public void testIntersectSmallerFullfilled()
  {
    SortedIntSet first = SortedIntSet.allValues(3);
    SortedIntSet second = new SortedIntSet(1, 2, 3, 5, 7);

    SortedIntSet bigger = first.copy();
    bigger.intersect(second);
    assertEquals(new SortedIntSet(1, 2), bigger);

    SortedIntSet lower = second.copy();
    lower.intersect(first);
    assertEquals(new SortedIntSet(1, 2), lower);
  }

  @Test
  public void testAppendSmallerFullfilled()
  {
    SortedIntSet first = SortedIntSet.allValues(3);
    SortedIntSet second = new SortedIntSet(5, 7, 9);

    SortedIntSet bigger = first.copy();
    bigger.union(second);
    assertEquals(new SortedIntSet(0, 1, 2, 5, 7, 9), bigger);

    SortedIntSet lower = second.copy();
    lower.union(first);
    assertEquals(new SortedIntSet(0, 1, 2, 5, 7, 9), lower);
  }

  @Test
  public void testRanges()
  {
    SortedIntSet fully = new SortedIntSet(1, 2, 3);
    List<Range<Integer>> ranges = fully.getRanges();
    assertEquals(1, ranges.size());
    assertEquals(new Range<>(1, 3), ranges.get(0));

    SortedIntSet gap = new SortedIntSet(1, 2, 4, 5);
    ranges = gap.getRanges();
    assertEquals(2, ranges.size());
    assertEquals(new Range<>(1, 2), ranges.get(0));
    assertEquals(new Range<>(4, 5), ranges.get(1));

    SortedIntSet border = new SortedIntSet(1, 3, 4, 6);
    ranges = border.getRanges();
    assertEquals(3, ranges.size());
    assertEquals(new Range<>(1, 1), ranges.get(0));
    assertEquals(new Range<>(3, 4), ranges.get(1));
    assertEquals(new Range<>(6, 6), ranges.get(2));
  }

  @Test
  public void testBug()
  {
    SortedIntSet res = new SortedIntSet(1, 3, 19, 21, 22);
    res.intersect(new SortedIntSet(Arrays.asList(19)));
    assertEquals(1, res.size());
    assertEquals(19, res.get(0));
  }

  @Test
  public void testRemoveAll()
  {
    SortedIntSet res = new SortedIntSet(85, 87, 96, 97);
    res.removeAll(new SortedIntSet(88, 89, 96));
    assertEquals(new SortedIntSet(85, 87, 97), res);

    res = new SortedIntSet(79, 81, 82, 83, 86, 88);
    res.removeAll(new SortedIntSet(71, 76, 79, 82, 96));
    assertEquals(new SortedIntSet(81, 83, 86, 88), res);
  }

  @Test
  public void testRemoveAllGenerated()
  {
    Random rnd = new Random();
    int iterations = 10000;
    while (--iterations >= 0)
    {
      SortedIntSet orig = genArray(rnd);
      SortedIntSet rem = genArray(rnd);
      SortedIntSet res = orig.copy();
      boolean isDel = res.removeAll(rem);
      assertEquals(isDel, res.size() < orig.size());
      for (Integer num: rem)
        if (res.contains(num))
          fail(orig.toString() + " - " + rem.toString() + " contains " + num + ", but should not");
      for (Integer num: orig)
        if (!rem.contains(num) && !res.contains(num))
          fail(orig.toString() + " - " + rem.toString() + " lost " + num);
    }
  }

  @Test
  public void testIntersectGenerated()
  {
    Random rnd = new Random();
    int iterations = 10000;
    while (--iterations >= 0)
    {
      SortedIntSet orig = genArray(rnd);
      SortedIntSet keep = genArray(rnd);
      SortedIntSet res = orig.copy();
      boolean isDel = res.intersect(keep);
      assertEquals(isDel, res.size() < orig.size());
      for (Integer num: orig)
        if (res.contains(num) != keep.contains(num))
        {
          if (res.contains(num))
            fail(orig.toString() + " - " + keep.toString() + " contains " + num + ", but should not");
          else
          fail(orig.toString() + " - " + keep.toString() + " lost " + num);
        }
    }
  }

  private SortedIntSet genArray(Random rnd)
  {
    int size = rnd.nextInt(100) + 1;
    SortedIntSet res = new SortedIntSet(size);
    for (int i = 0; i < size; ++i)
      res.add(rnd.nextInt(100));
    return res;
  }
}