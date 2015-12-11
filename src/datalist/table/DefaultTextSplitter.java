package datalist.table;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Default {@link TextSplitter} implementation.<br>
 * All chars except alphanumeric is separators. Chains with numbers merged to handle phones.<br><br>
 * Examples:<br>
 * Oki-doki -> ["oki", "doki"]<br>
 * 123-45-67 -> ["123", "45", "67", "4567", "1234567"] (phone)
 * @author SergeyT
 */
public class DefaultTextSplitter implements TextSplitter
{
  private final boolean ignoreCase;

  public DefaultTextSplitter(boolean ignoreCase)
  {
    this.ignoreCase = ignoreCase;
  }

  @SuppressWarnings("empty-statement")
  protected final boolean doSplit(Set<String> addTo, String text, boolean addSubNum)
  {
    if (text == null || text.isEmpty())
      return false;

    if (ignoreCase)
      text = text.toLowerCase();

    boolean res = false;
    boolean isNum, curNum;
    char ch;
    int pre = text.length();
    int cur = pre;
    StringBuilder nums = null;
    while (--cur >= 0)
    {
      if ((isNum = Character.isDigit((ch = text.charAt(cur)))) || Character.isAlphabetic(ch))
      {
        while (--cur >= 0)
        {
          ch = text.charAt(cur);
          if (!(curNum = Character.isDigit(ch)) && !Character.isAlphabetic(ch))
            break;
          isNum |= curNum;
        }
        String word = text.substring(cur+1, pre).toLowerCase();
        if (isNum)
        {
          if (nums == null)
            nums = new StringBuilder(word);
          else
            nums.insert(0, word);
          if (addSubNum)
          {
            addTo.add(nums.toString());
            res = true;
          }
        }
        else
        {
          if (nums != null && nums.length() > 0)
          {
            addTo.add(nums.toString());
            nums.setLength(0);
          }
          addTo.add(word);
          res = true;
        }
      }

      while (--cur >= 0 && !Character.isLetterOrDigit(text.charAt(cur)))
        /*nothing*/;
      pre = ++cur;
    }
    if (nums != null && nums.length() > 0)
    {
      addTo.add(nums.toString());
      res = true;
    }
    return res;
  }

  @Override
  public boolean splitText(Set<String> addTo, String text)
  {
    return doSplit(addTo, text, true);
  }

  @Override
  public Set<String> splitRequest(String request)
  {
    Set<String> addTo = new LinkedHashSet<>();
    doSplit(addTo, request, false);
    return addTo;
  }

  public static final DefaultTextSplitter CASE_INSENSITIVE = new DefaultTextSplitter(true);
  public static final DefaultTextSplitter CASE_SENSATIVE = new DefaultTextSplitter(false);
}
