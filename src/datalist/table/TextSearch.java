package datalist.table;

import datalist.list.DataList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Search table rows ({@link DataList} items with type {@link TableRow}) by words. Actually, by word prefixes.<br>
 * {@link TextSplitter} used to split text to words. TextSplitter might be case sensitive/insensitive, duplicate parts of text
 * to search by inner parts of words and other (for example, look at {@link DefaultTextSplitter})
 * @author SergeyT
 */
public class TextSearch<T extends TableRow>
{
  private final TextSearchCatalog<T> catalog;

  public TextSearch(DataList<T> list)
  {
    this(list, DefaultTextSplitter.CASE_INSENSITIVE, null);
  }

  public TextSearch(DataList<T> list, Set<Integer> notIndexingColumns)
  {
    this(list, DefaultTextSplitter.CASE_INSENSITIVE, notIndexingColumns);
  }

  public TextSearch(DataList<T> list, TextSplitter textSplitter, Set<Integer> notIndexingColumns)
  {
    if (list == null || textSplitter == null)
      throw new IllegalArgumentException();

    this.catalog = new TextSearchCatalog<>(list, textSplitter, notIndexingColumns);
  }

  /**
   * enum all results for text prefix. Returns null if word too short
   */
  public TextSearchResult findText(String text, Collection<Integer> skipColumns)
  {
    Collection<String> request = catalog.splitRequest(text);
    if (request.isEmpty())
      return TextSearchResult.empty();
    Iterator<String> it = request.iterator();
    TextSearchResult res = catalog.findAllByPrefix(it.next(), skipColumns);
    while (it.hasNext())
      res.intersect(catalog.findAllByPrefix(it.next(), skipColumns));
    return res;
  }
}