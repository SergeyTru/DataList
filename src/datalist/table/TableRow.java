package datalist.table;

/**
 * TableRow is useful to handle each list item as generic row. All fields accessed by column index.<br>
 * It can be used as a base class for objects with text search or for simplification of table-like output.<br>
 * @see SimpleTableRow
 * @author SergeyT
 */
public interface TableRow
{
  /** Copy of row data. You may modify this collection as you want */
  Comparable[] getData();

  /** Returns raw object by column index */
  Comparable getObject(int idx);

  /** Returns text representation by column index or null */
  String getText(int idx);
  
  /** Returns the number of columns in this row */
  int size();
}
