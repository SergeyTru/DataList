package datalist.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class DefaultTextSplitterTest
{
  @Test
  public void testText()
  {
    for (String text: Arrays.asList("РЕСПУБЛИКА УДМУРТСКАЯ", "  РЕСПУБЛИКА УДМУРТСКАЯ  "))
    {
      Set<String> coll = new HashSet<>();
      DefaultTextSplitter.CASE_INSENSITIVE.splitText(coll, text);
      Assert.assertEquals(coll.toString(), 2, coll.size());
      Assert.assertTrue(coll.toString(), coll.contains("республика"));
      Assert.assertTrue(coll.toString(), coll.contains("удмуртская"));

      coll = DefaultTextSplitter.CASE_INSENSITIVE.splitRequest(text);
      Assert.assertEquals(coll.toString(), 2, coll.size());
      Assert.assertTrue(coll.toString(), coll.contains("республика"));
      Assert.assertTrue(coll.toString(), coll.contains("удмуртская"));
    }
  }

  @Test
  public void testPhone()
  {
    Set<String> coll = new HashSet<>();
    Assert.assertTrue(DefaultTextSplitter.CASE_INSENSITIVE.splitText(coll, "(800) 123 45 67"));
    Assert.assertEquals(coll.toString(), 4, coll.size());
    Assert.assertTrue(coll.toString(), coll.contains("67"));
    Assert.assertTrue(coll.toString(), coll.contains("4567"));
    Assert.assertTrue(coll.toString(), coll.contains("1234567"));
    Assert.assertTrue(coll.toString(), coll.contains("8001234567"));

    coll.clear();
    Assert.assertTrue(DefaultTextSplitter.CASE_INSENSITIVE.splitText(coll, "(34345) 3-43-43"));
    Assert.assertEquals(coll.toString(), 4, coll.size());
    Assert.assertTrue(coll.toString(), coll.contains("43"));
    Assert.assertTrue(coll.toString(), coll.contains("4343"));
    Assert.assertTrue(coll.toString(), coll.contains("3434534343"));

    coll = DefaultTextSplitter.CASE_INSENSITIVE.splitRequest("(800) 123 45 67");
    Assert.assertEquals(coll.toString(), Collections.singleton("8001234567"), coll);

    coll = DefaultTextSplitter.CASE_INSENSITIVE.splitRequest("(34345) 3-43-43");
    Assert.assertEquals(coll.toString(), Collections.singleton("3434534343"), coll);
  }

  @Test
  public void testMixed()
  {
    for (String text: Arrays.asList("55-75 asdf 66-44 erty", "erty 55-75 asdf 66-44", "(55-75) asdf-66/44 erty", "erty /55/75/ asdf-66+44"))
    {
      Set<String> coll = new HashSet<>();
      DefaultTextSplitter.CASE_INSENSITIVE.splitText(coll, text);
      Assert.assertEquals(text + " | " + coll.toString(), 6, coll.size());
      Assert.assertTrue(coll.toString(), coll.contains("75"));
      Assert.assertTrue(coll.toString(), coll.contains("5575"));
      Assert.assertTrue(coll.toString(), coll.contains("asdf"));
      Assert.assertTrue(coll.toString(), coll.contains("44"));
      Assert.assertTrue(coll.toString(), coll.contains("6644"));
      Assert.assertTrue(coll.toString(), coll.contains("erty"));

      coll = DefaultTextSplitter.CASE_INSENSITIVE.splitRequest(text);
      Assert.assertEquals(text + " | " + coll.toString(), 4, coll.size());
      Assert.assertTrue(coll.toString(), coll.contains("5575"));
      Assert.assertTrue(coll.toString(), coll.contains("asdf"));
      Assert.assertTrue(coll.toString(), coll.contains("6644"));
      Assert.assertTrue(coll.toString(), coll.contains("erty"));
    }
  }

  @Test
  public void testNumWord()
  {
    for (String text: Arrays.asList("dom15"))
    {
      Set<String> coll = new HashSet<>();
      DefaultTextSplitter.CASE_INSENSITIVE.splitText(coll, text);
      Assert.assertEquals(text + " | " + coll.toString(), 1, coll.size());
      Assert.assertTrue(coll.toString(), coll.contains("dom15"));
    }
  }
}