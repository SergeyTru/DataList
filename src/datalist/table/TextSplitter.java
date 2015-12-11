package datalist.table;

import java.util.Set;

/**
 * TextSplitter extract words from text. Note that {@link TextSearch} search by prefixes and <tt>TextSplitter</tt> can duplicate word parts.
 * For example, look at {@link DefaultTextSplitter}<br>
 * Search request and text data might be handled differently.
 * @author SergeyT
 */
public interface TextSplitter
{
  /** Split request to words */
  Set<String> splitRequest(String request);
  /** Split text data and put all words to "addTo" */
  boolean splitText(Set<String> addTo, String text);
}
