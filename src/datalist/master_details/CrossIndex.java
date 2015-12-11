package datalist.master_details;

import datalist.io.ChannelBuilder;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Arrays;

/**
 * Stores one-to-many links from one table to another.<br>
 * Each storage file contains only one CrossIndex.<br>
 * To implement simple master-details table link, use {@link MasterDetailsList}<br>
 * In other cases, cross link might be implemented as follow:
 * <ul><li>call {@link add()} when items added to details table. Each array item contains index of item at master table.</li>
 * <li>to enum all details items for master item(s) call {@link oneToMany()}</li>
 * <li>to enum master item index(es) for details item(s), call {@link manyToOne()}</li></ul>
 */
public class CrossIndex
{
  private final File storageFile;
  private volatile int[] indexes;

  public CrossIndex(File storage) throws IOException
  {
    this.storageFile = storage;
    reload();
  }

  /** Load indexes from file */
  public final void reload()
  {
    if (storageFile.exists())
      try (ReadBuffer rb = new ChannelBuilder(storageFile).reader())
      {
        int cnt = (int)(rb.remaining() / 4);
        int[] new_indexes = new int[cnt];
        for (int i = 0; i < cnt; ++i)
          new_indexes[i] = rb.getInt();
        indexes = new_indexes;
      }
      catch (IOException ex)
      {
        throw new UncheckedIOException(ex);
      }
    else
      indexes = new int[0];
  }

  /** Returns all rows from details table by indexes at master table */
  public SortedIntSet oneToMany(SortedIntSet from)
  {
    SortedIntSet res = new SortedIntSet(from.size());
    int i = indexes.length;
    while (--i >= 0)
      if (from.contains(indexes[i]))
        res.add(i);
    return res.trim();
  }

  /** Returns distinct list of master table rows by details table row numbers */
  public SortedIntSet manyToOne(SortedIntSet from)
  {
    SortedIntSet res = new SortedIntSet(from.size());
    int i = from.size();
    while (--i >= 0)
      res.add(indexes[from.get(i)]);
    return res.trim();
  }

  /** Master table row num by details table row num */
  public int manyToOne(int from)
  {
    return indexes[from];
  }

  /** Removes all from 'one' if it contains nothing on 'many' and remove all from 'many' if 'one' does not contains correspond item */
  public void intersect(SortedIntSet one, SortedIntSet many)
  {
    if (one.isEmpty())
    {
      many.clear();
      return;
    }
    //'one's that founds by 'many's
    SortedIntSet onesFromMany = manyToOne(many);
    //keep only requires 'one's
    if (one.intersect(onesFromMany))
      if (one.isEmpty())
      {
        many.clear(); //does not intersected at all
        return;
      }
    //found companies that should be removed
    onesFromMany.removeAll(one);
    //and remove 'many's
    many.removeIf(manyVal -> onesFromMany.contains(indexes[manyVal]));
  }

  /** Count of rows at details table */
  public int size()
  {
    return indexes.length;
  }

  /** Clean up index and file */
  void clean()
  {
    indexes = new int[0];
    try (FileChannel fc = FileChannel.open(storageFile.toPath(), WRITE, CREATE))
    {
      fc.truncate(0);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  /**
   * Add 'cnt' items from 'items' to the end of list.<br>
   * For m = items[d], d is row index at details table, m is index at master table.
   */
  public void add(int[] items, int cnt)
  {
    int[] newIndexes = Arrays.copyOf(indexes, indexes.length + cnt);
    System.arraycopy(items, 0, newIndexes, indexes.length, cnt);
    indexes = newIndexes;
    appendToFile(items, cnt);
  }

  private void appendToFile(int[] items, int cnt)
  {
    try (WriteBuffer wb = new ChannelBuilder(storageFile).writer())
    {
      for (int i = 0; i < cnt; ++i)
        wb.putInt(items[i]);
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }
}