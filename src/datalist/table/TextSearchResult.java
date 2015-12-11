package datalist.table;

import datalist.types.SortedIntSet;

/**
 * Result of search.
 */
public class TextSearchResult
{
  private final SortedIntSet columns;
  private final SortedIntSet rows;
  private final SortedIntSet skippedColumns;

  TextSearchResult(SortedIntSet columns, SortedIntSet rows, SortedIntSet skippedColumns)
  {
    this.columns = columns;
    this.rows = rows;
    this.skippedColumns = skippedColumns;
  }

  public static TextSearchResult empty()
  {
    return new TextSearchResult(SortedIntSet.empty(), SortedIntSet.empty(), SortedIntSet.empty());
  }

  public static TextSearchResult allRows(int size)
  {
    return new TextSearchResult(SortedIntSet.empty(), SortedIntSet.allValues(size), SortedIntSet.empty());
  }

  /** Columns in which the text was found */
  public SortedIntSet getColumns()
  {
    return columns;
  }

  /** Row numbers with founded text */
  public SortedIntSet getRows()
  {
    return rows;
  }

  /** Columns that contain the search text, but were skiped */
  public SortedIntSet getSkippedColumns()
  {
    return skippedColumns;
  }

  /** For multiword search: the intersection contains only rows containing all the words */
  void intersect(TextSearchResult other)
  {
    rows.intersect(other.rows);
    columns.union(other.columns);
    skippedColumns.union(other.skippedColumns);
  }
}
