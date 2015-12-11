package datalist.master_details;

import datalist.index.Index;
import datalist.list.DataList;
import datalist.list.Where;
import datalist.types.Range;
import datalist.types.SortedIntSet;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Master-details implementation for {@link datalist.list.Where}, that allows to filter both lists.<br>
 * Use indexes from any list, <tt>MasterDetailsWhere</tt> filter items at other list using {@link CrossIndex}
 * @author SergeyT
 */
public class MasterDetailsWhere
{
  private final Where masterWhere;
  private final Where detailsWhere;
  private boolean crossApplied;
  private final CrossIndex cross;

  public MasterDetailsWhere(DataList master, DataList details, CrossIndex cross)
  {
    this.masterWhere = new Where(master);
    this.detailsWhere = new Where(details);
    this.cross = cross;
  }

  public <T extends Comparable<T>> MasterDetailsWhere and(Index<T> index, T value)
  {
    crossApplied = false;
    if (masterWhere.indexApplyable(index))
      masterWhere.and(index, value);
    else if (detailsWhere.indexApplyable(index))
      detailsWhere.and(index, value);
    else
      throw new IllegalArgumentException("Index not applyable for tables");
    return this;
  }

  public <T extends Comparable<T>> MasterDetailsWhere and(Index<T> index, T min, T max)
  {
    crossApplied = false;
    if (masterWhere.indexApplyable(index))
      masterWhere.and(index, min, max);
    else if (detailsWhere.indexApplyable(index))
      detailsWhere.and(index, min, max);
    else
      throw new IllegalArgumentException("Index not applyable for tables");
    return this;
  }

  public <T extends Comparable<T>> MasterDetailsWhere and(Index<T> index, Collection<T> values)
  {
    crossApplied = false;
    if (masterWhere.indexApplyable(index))
      masterWhere.and(index, values);
    else if (detailsWhere.indexApplyable(index))
      detailsWhere.and(index, values);
    else
      throw new IllegalArgumentException("Index not applyable for tables");
    return this;
  }

  public <T extends Comparable<T>> MasterDetailsWhere and(Index<T> index, Predicate<T> condition, boolean includeNull)
  {
    crossApplied = false;
    if (masterWhere.indexApplyable(index))
      masterWhere.and(index, condition, includeNull);
    else if (detailsWhere.indexApplyable(index))
      detailsWhere.and(index, condition, includeNull);
    else
      throw new IllegalArgumentException("Index not applyable for tables");
    return this;
  }
  
  public <T extends Comparable<T>> MasterDetailsWhere and(Index<T> index, Range<T>... ranges)
  {
    crossApplied = false;
    if (masterWhere.indexApplyable(index))
      masterWhere.and(index, ranges);
    else if (detailsWhere.indexApplyable(index))
      detailsWhere.and(index, ranges);
    else
      throw new IllegalArgumentException("Index not applyable for tables");
    return this;
  }
  
  public <T extends Comparable<T>> MasterDetailsWhere andIndexInList(SortedIntSet internal, boolean isMasterTable)
  {
    crossApplied = false;
    if (isMasterTable)
      masterWhere.andIndexInList(internal);
    else
      detailsWhere.andIndexInList(internal);
    return this;
  }

  public MasterDetailsIndexes getIndexes()
  {
    if (!crossApplied)
    {
      cross.intersect(masterWhere.getIndexes(), detailsWhere.getIndexes());
      crossApplied = true;
    }
    return new MasterDetailsIndexes(masterWhere.getIndexes(), detailsWhere.getIndexes());
  }

  public boolean isEmpty()
  {
    if (!crossApplied)
    {
      cross.intersect(masterWhere.getIndexes(), detailsWhere.getIndexes());
      crossApplied = true;
    }
    return detailsWhere.isEmpty();
  }

  @Override
  public String toString()
  {
    return masterWhere + "; " + detailsWhere;
  }
}
