package datalist.types;

/** int[] array with/without fixed order */
public interface IntArray extends Iterable<Integer>
{
  boolean contains(int val);
  int get(int index);
  boolean isEmpty();
  int size();
}
