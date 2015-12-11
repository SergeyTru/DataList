package datalist.table;

import datalist.master_details.CrossIndex;
import datalist.master_details.MasterDetailsList;
import java.util.Collection;
import java.util.Iterator;

/**
 * MasterDetailsTextSearch is {@link TextSearch} for {@link MasterDetailsList}. The text is searched in both tables.<br>
 * Filtered rows contain every searched word at master or details table.<br><br>
 * For example: at books list (details) with link to authors list (master) we could call
 * <pre>findText("Bloch progra", null, null)</pre> to find Josh Bloch (master table "authors") book
 * "Effecting Java Programmig Language Guide" (details table "books")
 * @author SergeyT
 */
public class MasterDetailsTextSearch
{
  private final TextSearchCatalog<?> master;
  private final TextSearchCatalog<?> details;
  private final CrossIndex cross;

  public <M extends TableRow, S extends TableRow> MasterDetailsTextSearch(MasterDetailsList<M, S, ?> list)
  {
    this(list, null, null);
  }

  public <M extends TableRow, S extends TableRow> MasterDetailsTextSearch(MasterDetailsList<M, S, ?> list,
        Collection<Integer> notIndexingMasterColumns, Collection<Integer> notIndexingDetailsColumns)
  {
    this(list, DefaultTextSplitter.CASE_INSENSITIVE, null, null);
  }

  public <M extends TableRow, S extends TableRow> MasterDetailsTextSearch(MasterDetailsList<M, S, ?> list, TextSplitter textSplitter,
        Collection<Integer> notIndexingMasterColumns, Collection<Integer> notIndexingDetailsColumns)
  {
    master = new TextSearchCatalog<>(list.getMasterList(), textSplitter, notIndexingMasterColumns);
    details = new TextSearchCatalog<>(list.getDetailsList(), textSplitter, notIndexingDetailsColumns);
    cross = list.getCrossIndex();
  }
  
  public MasterDetailsSearchResult findText(String text, Collection<Integer> skipColumnsMaster, Collection<Integer> skipColumnsDetails)
  {
    Collection<String> request = master.splitRequest(text);
    if (request.isEmpty())
      return MasterDetailsSearchResult.empty();

    Iterator<String> it = request.iterator();
    String word = it.next();
    TextSearchResult masterRes = master.findAllByPrefix(word, skipColumnsMaster);
    TextSearchResult detailsRes = details.findAllByPrefix(word, skipColumnsDetails);
    while (it.hasNext())
    {
      word = it.next();
      final TextSearchResult oneWordMaster = master.findAllByPrefix(word, skipColumnsMaster);
      final TextSearchResult oneWordDetails = details.findAllByPrefix(word, skipColumnsDetails);
      oneWordDetails.getRows().union(cross.oneToMany(oneWordMaster.getRows()));
      masterRes.intersect(oneWordMaster);
      detailsRes.intersect(oneWordDetails);
    }
    return new MasterDetailsSearchResult(masterRes, detailsRes);
  }
}
