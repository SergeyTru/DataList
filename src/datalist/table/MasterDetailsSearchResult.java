package datalist.table;

import datalist.master_details.MasterDetailsIndexes;

/**
 * MasterDetailsSearchResult class holds search result for both tables, that useful for {@link MasterDetailsList} usage.<br><br>
 * MasterDetailsSearchResult extends {@link MasterDetailsIndexes}, that allows intersect filter and search results.
 * @author SergeyT
 */
public class MasterDetailsSearchResult extends MasterDetailsIndexes
{
  private final TextSearchResult masterResult, detailsResult;

  public MasterDetailsSearchResult(TextSearchResult master, TextSearchResult details)
  {
    super(master.getRows(), details.getRows());
    this.masterResult = master;
    this.detailsResult = details;
  }

  public static MasterDetailsSearchResult empty()
  {
    return new MasterDetailsSearchResult(TextSearchResult.empty(), TextSearchResult.empty());
  }

  public TextSearchResult getMasterSearchResult()
  {
    return masterResult;
  }

  public TextSearchResult getDetailsSearchResult()
  {
    return detailsResult;
  }
}
