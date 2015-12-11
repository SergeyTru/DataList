package datalist.master_details;

import datalist.types.SortedIntSet;

/**
 * MasterDetailsIndexes class holds items indexes on both tables, that useful for {@link MasterDetailsList} usage
 * @author SergeyT
 */
public class MasterDetailsIndexes
{
  private final SortedIntSet master;
  private final SortedIntSet details;

  public MasterDetailsIndexes(SortedIntSet master, SortedIntSet details)
  {
    this.master = master;
    this.details = details;
  }

  public static MasterDetailsIndexes empty()
  {
    return new MasterDetailsIndexes(SortedIntSet.empty(), SortedIntSet.empty());
  }

  public static MasterDetailsIndexes allValues(int masterSize, int detailsSize)
  {
    return new MasterDetailsIndexes(SortedIntSet.allValues(masterSize), SortedIntSet.allValues(detailsSize));
  }

  public SortedIntSet getMasterIndexes()
  {
    return master;
  }

  public SortedIntSet getDetailsIndexes()
  {
    return details;
  }

  public MasterDetailsIndexes createIntersection(MasterDetailsIndexes other)
  {
    SortedIntSet tmpMaster = master.copy();
    tmpMaster.intersect(other.master);
    SortedIntSet tmpDetails = details.copy();
    tmpDetails.intersect(other.details);
    return new MasterDetailsIndexes(tmpMaster, tmpDetails);
  }
}
