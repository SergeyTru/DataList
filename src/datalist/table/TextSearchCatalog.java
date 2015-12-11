package datalist.table;

import datalist.io.ChannelBuilder;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DataList;
import datalist.list.DataListChangedListener;
import datalist.list.Database;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Each <tt>TextSearchCatalog</tt> store rows/columns for each word. Use {@link TextSearch} to find at single table or
 * {@link MasterDetailsTextSearch} for master-details tables.
 * @author SergeyT
 * @param <T> is tpe of list, not required, but simplify code.
 */
class TextSearchCatalog<T extends TableRow>
{
  private static final int prefixLength = 2;
  private final DataList<T> list;
  private final NavigableMap<String, Long> catalog;
  private final FileChannel fc;
  private final File catalogFile;
  private TextSplitter splitter;

  TextSearchCatalog(DataList<T> list, TextSplitter splitter, Collection<Integer> notIndexingColumns)
  {
    this.list = list;
    try
    {
      Database db = list.getDatabase();
      this.fc = new ChannelBuilder(db.getTextSearchFile(list.getTableName())).forRead().forWrite().build();
      this.catalogFile = db.getTextSearchIndexFile(list.getTableName());
      this.catalog = readCatalog(this.catalogFile);
      this.splitter = splitter;
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
    list.addListener(new DataListChangedListener<T>()
    {
      @Override
      public void cleaned(DataList<T> list)
      {
        try
        {
          fc.truncate(0);
        }
        catch (IOException ex)
        {
          throw new UncheckedIOException(ex);
        }
      }

      @Override
      public void dataAdded(DataList<T> list, int from, int till)
      {
        reindex(splitter, notIndexingColumns);
      }
    });
  }

  private NavigableMap<String, Long> readCatalog(File catalogFile)
  {
    TreeMap<String, Long> result = new TreeMap<>();
    if (catalogFile.exists())
      try (final ReadBuffer rb = new ChannelBuilder(catalogFile).reader())
      {
        while (rb.hasRemaining())
        {
          String pref = rb.getString();
          long offs = rb.getLong();
          if (result.put(pref, offs) != null)
            System.err.println("Duplicate " + pref);
        }
      }
      catch (IOException ex)
      {
        throw new UncheckedIOException(ex);
      }
    return result;
  }

  public Collection<String> splitRequest(String request)
  {
    return request != null? splitter.splitRequest(request) : Collections.emptySet();
  }

  public TextSearchResult findAllByPrefix(String word, Collection<Integer> skipColumns)
  {
    if (word == null)
      throw new IllegalArgumentException();

    if (skipColumns == null)
      skipColumns = Collections.emptySet();

    String curPrefix = word.substring(0, prefixLength);
    Long offsObj = catalog.get(curPrefix);
    if (offsObj == null)
      return new TextSearchResult(SortedIntSet.empty(), SortedIntSet.empty(), SortedIntSet.empty());
    Map.Entry<String, Long> higher = catalog.higherEntry(word);

    try
    {
      long from = offsObj;
      long till = higher == null? fc.size() : higher.getValue();

      int approx = (int)((till - from) / 8);
      SortedIntSet cols = new SortedIntSet(approx);
      SortedIntSet rows = new SortedIntSet(approx);
      SortedIntSet skips = new SortedIntSet(skipColumns.size() + 1); //speed up cheat
      try (ReadBuffer rdr = new ReadBuffer(fc, -1))
      {
        rdr.position(from);
        while (rdr.position() < till)
        {
          String curWord = rdr.getString();
          if (curWord.startsWith(word))
            readPositions(rdr, cols, rows, skips, skipColumns);
          else
            skipPositions(rdr);
        }
        if (rdr.position() != till)
          throw new IllegalStateException("text index corrupted");
      }
      return new TextSearchResult(cols.trim(), rows.trim(), skips.trim());
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  private void skipPositions(ReadBuffer rdr)
  {
    int cnt = rdr.getInt();
    rdr.position(rdr.position() + cnt * 8);
  }

  private void readPositions(ReadBuffer rdr, SortedIntSet cols, SortedIntSet rows, SortedIntSet skipCols, Collection<Integer> skipColumns)
  {
    int col, row;
    int cnt = rdr.getInt();
    while (--cnt >= 0)
    {
      row = rdr.getInt();
      col = rdr.getInt();
      if (!skipColumns.contains(col))
      {
        rows.add(row);
        cols.add(col);
      }
      else
        skipCols.add(col);
    }
  }

  public void reindex(TextSplitter textSplitter, Collection<Integer> ignoreColumns)
  {
    try
    {
      if (ignoreColumns == null)
        ignoreColumns = Collections.emptyList();
      DataBuilder builder = new DataBuilder(prefixLength);
      Set<String> allTexts = new HashSet<>();
      int len = list.size();
      for (int i = 0; i < len; ++i)
      {
        T line = list.get(i);
        for (int col = 0; col < line.size(); ++col)
        {
          if (ignoreColumns.contains(col))
            continue;
          if (!textSplitter.splitText(allTexts, line.getText(col)))
            continue;
          for (String text : allTexts)
            if (text.length() >= prefixLength)
              builder.addWord(text, i, col);
          allTexts.clear();
        }
      }
      catalog.clear();
      fc.truncate(0);
      try (final WriteBuffer dataWriter = new WriteBuffer(fc, -1); final WriteBuffer catalogWriter = new ChannelBuilder(catalogFile).writer())
      {
        catalogWriter.position(0);
        builder.complete(dataWriter, catalogWriter, catalog);
        catalogWriter.truncateTail();
      }
    }
    catch (IOException ex)
    {
      throw new UncheckedIOException(ex);
    }
  }

  private static class DataBuilder
  {
    //  Each ROWS_TO_CHECK rows we check for lines with low frequency and flush them to files. It helps to reduce memory
    // consumption. To avoid removing text contained in the last visited row and few next rows, we flush only texts that
    // absent at last KEEP_ROWS_INTERVAL rows.
    //  Low frequency text is text, that meets less than CNT/COUNT_DEVIDER times in CNT rows.

    private static final int ROWS_TO_CHECK = Integer.getInteger("text_search.catalog.reindex.rows_to_check", 16384);
    private static final int KEEP_ROWS_INTERVAL = Integer.getInteger("text_search.catalog.reindex.texts_keep_interval_in_rows", 10000);
    private static final int COUNT_DEVIDER = Integer.getInteger("text_search.catalog.reindex.count_devider_to_keep", 100);
    private final Map<String, SimpleIntList> offsByWord = new HashMap<>(2048);
    private final Map<String, FileChannel> temporaryFiles = new HashMap<>(128);
    private final int prefixLen;
    private int nextCheck = ROWS_TO_CHECK;

    private DataBuilder(int prefixLen)
    {
      this.prefixLen = prefixLen;
    }

    public void addWord(String text, int row, int col) throws IOException
    {
      SimpleIntList allRowCol = offsByWord.get(text);
      if (allRowCol == null)
        offsByWord.put(text, allRowCol = new SimpleIntList());
      allRowCol.add(row, col);
      if (row >= nextCheck)
      {
        flushRareTexts(row - 1 - KEEP_ROWS_INTERVAL, (row - 1) / COUNT_DEVIDER); //"row - 1" because this row just starts, call with previous, fully complete row
        nextCheck += ROWS_TO_CHECK;
      }
    }

    private void flushRareTexts(int skipAfterRow, int minimalCountToKeep) throws IOException
    {
      minimalCountToKeep *= 2; //SimpleIntList size is doubled because of 2 coordinates
      Map<String, Map<String, SimpleIntList>> foundByPrefix = null;
      Iterator<Map.Entry<String, SimpleIntList>> it = offsByWord.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry<String, SimpleIntList> word = it.next();
        if (word.getValue().lastRow() <= skipAfterRow && word.getValue().size < minimalCountToKeep)
        {
          it.remove();
          if (foundByPrefix == null)
            foundByPrefix = new HashMap<>();
          addWordByPrefix(foundByPrefix, word);
        }
      }
      if (foundByPrefix == null)
        return;
      for (Map.Entry<String, Map<String, SimpleIntList>> entry : foundByPrefix.entrySet())
      {
        FileChannel fc = temporaryFiles.get(entry.getKey());
        if (fc == null)
          temporaryFiles.put(entry.getKey(), fc = ChannelBuilder.temporary("text_search_", "_" + entry.getKey()).forRead().forWrite().build());
        try (final WriteBuffer wb = new WriteBuffer(fc, -1))
        {
          for (Map.Entry<String, SimpleIntList> pair : entry.getValue().entrySet())
            write(wb, pair.getKey(), pair.getValue());
        }
      }
    }

    private void addWordByPrefix(Map<String, Map<String, SimpleIntList>> addTo, Map.Entry<String, SimpleIntList> word)
    {
      String prefix = word.getKey().substring(0, prefixLen);
      Map<String, SimpleIntList> list = addTo.get(prefix);
      if (list == null)
        addTo.put(prefix, list = new HashMap<>());
      list.put(word.getKey(), word.getValue());
    }

    private void write(WriteBuffer wb, String word, SimpleIntList list) throws IOException
    {
      wb.putString(word);
      final int len = list.size;
      final int[] idxs = list.data;
      if ((len & 1) != 0)
        throw new IllegalStateException();
      wb.putInt(len / 2); //pairs count
      for (int i = 0; i < len; ++i)
        wb.putInt(idxs[i]);
    }

    @SuppressWarnings(value = "UnusedAssignment")
    public void complete(WriteBuffer dataWriter, WriteBuffer catalogWriter, Map<String, Long> catalog) throws IOException
    {
      Map<String, Map<String, SimpleIntList>> byPrefixes = new HashMap<>();
      Iterator<Map.Entry<String, SimpleIntList>> it = offsByWord.entrySet().iterator();
      while (it.hasNext())
      {
        addWordByPrefix(byPrefixes, it.next());
        it.remove(); //mark as garbage
      }
      Set<String> prefixSet = new HashSet<>(temporaryFiles.keySet());
      prefixSet.addAll(byPrefixes.keySet());
      List<String> prefixes = new ArrayList<>(prefixSet);
      prefixSet = null; //mark as garbage
      prefixes.sort(null);
      for (String prefix : prefixes)
      {
        catalog.put(prefix, dataWriter.position());
        catalogWriter.putString(prefix);
        catalogWriter.putLong(dataWriter.position());
        Map<String, SimpleIntList> inMem = byPrefixes.get(prefix);
        if (inMem != null)
          for (Map.Entry<String, SimpleIntList> pair : inMem.entrySet())
            write(dataWriter, pair.getKey(), pair.getValue());
        FileChannel tmpFile = temporaryFiles.get(prefix);
        if (tmpFile != null)
          dataWriter.copyAllFrom(tmpFile);
      }
      for (FileChannel tmp : temporaryFiles.values())
        tmp.close();
      temporaryFiles.clear();
    }
  }

  private static class SimpleIntList
  {
    int[] data = new int[8]; //enough for more than 90% of data
    int size;

    void add(int row, int col)
    {
      if (data.length < size + 2)
        data = Arrays.copyOf(data, data.length * 2);
      data[size++] = row;
      data[size++] = col;
    }

    int lastRow()
    {
      return size == 0 ? 0 : data[size - 2];
    }

    @Override
    public String toString()
    {
      StringBuilder sb = new StringBuilder();
      sb.append('[');
      for (int i = 0; i < size; ++i)
        sb.append(data[i]).append(", ");
      sb.setLength(sb.length() - 2);
      return sb.append(']').toString();
    }
  }
}
//
//  public static void main(String[] args) throws IOException
//  {
//    Database db = new Database(new File("work"));
//    final DataTable<DeclData> declTable = new DataTable<>(db, "declarations", DeclData.streamer);
//    declTable.textSearch().reindex(declTable, null);
//    final DataTable<CompanyData> compTable = new DataTable<>(db, "companies", new CompanyData.Streamer());
//    compTable.textSearch().reindex(compTable, null);
//  }
