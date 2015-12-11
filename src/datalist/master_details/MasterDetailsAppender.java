package datalist.master_details;

import datalist.list.DataList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MasterDetailsAppender is common appender for {@link MasterDetailsList}<br>
 * Most useful for import from plain format, when master items copied to each details item. This class deduplicates master items.<br><br>
 * Use this class instead of adding data directly to lists to keep {@link CrossIndex} valid.<br>
 * You may override {@link onOtherDataSameHash()} to handle collision. By default, collisions are logged, but first item is used.<br>
 * 
 * @author SergeyT
 * @param <M> the type of items at master table
 * @param <D> the type of items at details table
 * @param <K> the type of unique key
 */
public class MasterDetailsAppender<M, D, K> implements AutoCloseable
{
  private static final Logger LOG = Logger.getLogger(MasterDetailsAppender.class.getName());

  private final MasterDetailsList<M, D, ?> list;
  private final DataList<M>.DataAppender masterAppender;
  private final DataList<D>.DataAppender detailAppender;
  private final Function<M, K> masterToKey;

  private final Map<K, MasterItemInfo> masterKeys;
  private final int[] idxs;
  private int idxPos;

  /**
   * Constructs appender for <tt>MasterDetailsList</tt> that commit changes each <tt>chunkSize</tt> items.
   * @param list - master/details list for append data
   * @param masterToKey - function, that generate key from master item
   * @param chunkSize
   */
  public MasterDetailsAppender(MasterDetailsList<M, D, ?> list, Function<M, K> masterToKey, int chunkSize)
  {
    if (chunkSize <= 0)
      throw new IllegalArgumentException("chunkSize=" + chunkSize + ", but it should be bigger than 0");
    this.list = list;
    this.masterToKey = masterToKey;
    this.masterKeys = addFromList(list.getMasterList());
    this.masterAppender = list.getMasterList().getAppender(chunkSize);
    this.detailAppender = list.getDetailsList().getAppender(chunkSize);
    this.idxs = new int[chunkSize];
  }

  @Override
  public void close()
  {
    if (idxPos > 0)
    {
      list.getCrossIndex().add(idxs, idxPos);
      idxPos = 0;
    }
    masterAppender.close();
    detailAppender.close();
  }

  private Map<K, MasterItemInfo> addFromList(DataList<M> list)
  {
    int len = list.size();
    Map<K, MasterItemInfo> result = new HashMap<>(len);
    for (int i = 0; i < len; ++i)
    {
      final M item = list.get(i);
      if (result.put(masterToKey.apply(item), new MasterItemInfo(i, item==null?0:item.hashCode())) != null)
        throw new IllegalStateException("Multiple items with same key");
    }
    return result;
  }

  public int addItem(M master, D details)
  {
    K key = masterToKey.apply(master);
    MasterItemInfo info = masterKeys.get(key);
    int masterIndex;
    if (info != null)
    {
      masterIndex = info.idx;
      if (info.hash != (master==null?0:master.hashCode()))
      {
        int newIndex = onOtherDataSameHash(master, masterIndex);
        if (newIndex == -1)
          masterIndex = masterAppender.addItem(master);
        else if (newIndex > list.getMasterList().size())
          throw new IllegalArgumentException("Index " + newIndex + " is out of range");
        masterKeys.put(key, new MasterItemInfo(masterIndex, master==null?0:master.hashCode()));
      }
    }
    else
    {
      masterIndex = masterAppender.addItem(master);
      masterKeys.put(key, new MasterItemInfo(masterIndex, master==null?0:master.hashCode()));
    }
    if (idxPos == idxs.length)
    {
      list.getCrossIndex().add(idxs, idxPos);
      idxPos = 0;
    }
    idxs[idxPos++] = masterIndex;
    return detailAppender.addItem(details);
  }

  /**
   * Override onOtherDataSameHash() to handle key duplication at master table. Default implementation just log warning.
   * @param newMasterItem - item, that have same value of masterToKey() value, but other hash
   * @param oldIndex - index of previous item
   * @return index on master table or -1 to add this master item
   */
  protected int onOtherDataSameHash(M newMasterItem, int oldIndex)
  {
    LOG.log(Level.WARNING, "Another item with same key, but other hash: {0}", newMasterItem);
    return oldIndex;
  }

  private static class MasterItemInfo
  {
    final int idx;
    final int hash;

    public MasterItemInfo(int idx, int hash)
    {
      this.idx = idx;
      this.hash = hash;
    }
  }
}
