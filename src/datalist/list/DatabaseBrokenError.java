package datalist.list;

public class DatabaseBrokenError extends RuntimeException
{
  public DatabaseBrokenError(String message)
  {
    super(message);
  }

  public DatabaseBrokenError(String message, Throwable cause)
  {
    super(message, cause);
  }
}
