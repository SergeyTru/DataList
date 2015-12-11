package examples.speed;

import datalist.index.SortedIndex;
import datalist.list.DataList;
import datalist.list.Database;
import datalist.master_details.MasterDetailsAppender;
import datalist.master_details.MasterDetailsIndexes;
import datalist.master_details.MasterDetailsList;
import datalist.stream.DBDataStreamers;
import datalist.types.Range;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import model.CompanyData;
import model.DeclData;
import model.DeclarationRow;

public class TestImportList
{
  private final static int READ_BLOCK_SIZE = 200;

  public static void main(String[] args) throws IOException, Exception
  {
    final File dbDir = new File("speed_db");
    cleanUp(dbDir);
    Database db = new Database(dbDir);
    SortedIndex<Date> masterIndex = new SortedIndex<>(new File(db.getDirectory(), "master-index"), DBDataStreamers.dates);
    SortedIndex<Long> detailsIndex = new SortedIndex<>(new File(db.getDirectory(), "details-index"), DBDataStreamers.longs_with_nulls);
    DataList<CompanyData> master = new DataList<>(db, "master", new CompanyData.Streamer()).withIndex(masterIndex, x->(Date)x.getObject(5), true);
    DataList<DeclData> details = new DataList<>(db, "slave", DeclData.streamer).withIndex(detailsIndex, x -> x.getTNVED(), true);
    MasterDetailsList<CompanyData, DeclData, DeclarationRow> data = new MasterDetailsList<>(master, details, (x, y) -> new DeclarationRow(y, x));
    try (CsvImport importer = new CsvImport(new File("ExcelReconvertor/all.csv").getAbsoluteFile()))
    {
      loadData(data, importer);
    }
    long preTime = System.currentTimeMillis();
    MasterDetailsIndexes idx = data.where(detailsIndex, new Range<>(0L, 3100000000L), new Range<>(3199999999L, Long.MAX_VALUE))
        .and(masterIndex, new GregorianCalendar(1990, 00, 01).getTime(), new GregorianCalendar(2070, 00, 01).getTime()).getIndexes();
    System.out.println("Find data: " + (System.currentTimeMillis() - preTime) + " ms");
    System.out.printf("%d/%d at master, %d/%d at details%n",
      idx.getMasterIndexes().size(), master.size(), idx.getDetailsIndexes().size(), details.size());
    System.out.flush();

    preTime = System.currentTimeMillis();
    List<DeclarationRow> subData = data.sublist(idx.getDetailsIndexes());
    int pos = 0;
    while (pos < subData.size())
    {
      int till = Math.min(pos + READ_BLOCK_SIZE, subData.size());
      for (int i = pos; i < till; ++i)
        if (Double.valueOf(Double.POSITIVE_INFINITY) == subData.get(i).getObject(0))
          throw new IllegalStateException("Never happends");
      pos = till;
    }
    System.out.println("Fetch by " + READ_BLOCK_SIZE + ": " + (System.currentTimeMillis() - preTime) + " ms");
  }

  private static void loadData(MasterDetailsList<CompanyData, DeclData, DeclarationRow> data, final CsvImport importer) throws IOException
  {
    MasterDetailsAppender<CompanyData, DeclData, Long> appender = new MasterDetailsAppender<>(data, x->x.getINN(), 100000);
    int cnt = 2000000;
    long preTime = System.currentTimeMillis();
    while (--cnt >= 0)
    {
      List<Comparable> line = importer.readLine();
      final List<Comparable> subList = line.subList(3, 16);
      final CompanyData companyData = new CompanyData(subList.toArray(new Comparable[subList.size()]));
      subList.clear(); //remove company data
      final DeclData declData = new DeclData(line.toArray(new Comparable[line.size()]));
      appender.addItem(companyData, declData);
    } System.out.println("First import: " + (System.currentTimeMillis() - preTime) + " ms");
    System.out.flush();

    preTime = System.currentTimeMillis();
    appender.close();
    System.out.println("Build indexes: " + (System.currentTimeMillis() - preTime) + " ms");
    System.out.flush();

    preTime = System.currentTimeMillis();
    appender = new MasterDetailsAppender<>(data, x->x.getINN(), 100000);
    cnt = 0;
    while (true)
    {
      List<Comparable> line = importer.readLine();
      if (line == null)
        break;
      final List<Comparable> subList = line.subList(3, 16);
      final CompanyData companyData = new CompanyData(subList.toArray(new Comparable[subList.size()]));
      subList.clear(); //remove company data
      final DeclData declData = new DeclData(line.toArray(new Comparable[line.size()]));
      appender.addItem(companyData, declData);
      ++cnt;
    }
    System.out.println("Append next " + cnt + ": " + (System.currentTimeMillis() - preTime) + " ms");
    System.out.flush();
    
    preTime = System.currentTimeMillis();
    appender.close();
    System.out.println("Build indexes: " + (System.currentTimeMillis() - preTime) + " ms");
    System.out.flush();
  }

  private static void cleanUp(File dbDir)
  {
    if (!dbDir.exists() || !dbDir.isDirectory())
      return;
    for (File file: dbDir.listFiles())
    {
      if (file.isDirectory())
        cleanUp(file);
      file.delete();
    }
  }
}
