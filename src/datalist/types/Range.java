package datalist.types;

import java.util.Objects;

public class Range<T extends Comparable<T>>
{
  private final T min;
  private final T max;

  public Range(T min, T max)
  {
    this.min = min;
    this.max = max;
  }

  public T getMin()
  {
    return min;
  }

  public T getMax()
  {
    return max;
  }

  public boolean isValid()
  {
    return min.compareTo(max) <= 0;
  }

  @Override
  public String toString()
  {
    return "[" + min + " - " + max + ']';
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(this.min) * 53 + Objects.hashCode(this.max);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null || getClass() != obj.getClass())
      return false;
    final Range<?> other = (Range<?>) obj;
    return Objects.equals(this.min, other.min) && Objects.equals(this.max, other.max);
  }

  public boolean includeRange(Range<T> range)
  {
    return min.compareTo(range.min) <= 0 && max.compareTo(range.max) >= 0;
  }

  public boolean includeValue(T value)
  {
    return min.compareTo(value) <= 0 && max.compareTo(value) >= 0;
  }
}
