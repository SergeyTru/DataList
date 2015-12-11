package datalist.list;

/**
 * To minimize read count till rebuild indexes, DataListChangedListener and DataListChangedListener.InfoCollector used.<br>
 * When all items added, DataList call DataListChangedListener.InfoCollector.nextItem(T) for each item at each index.<br>
 * @author SergeyT
 */
public interface DataListChangedListener<T>
{
  public void cleaned(DataList<T> list);
  public void dataAdded(DataList<T> list, int from, int till);
}
