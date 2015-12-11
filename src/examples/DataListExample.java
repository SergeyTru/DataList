package examples;

import datalist.index.SetIndex;
import datalist.index.SortedIndex;
import datalist.io.ReadBuffer;
import datalist.io.WriteBuffer;
import datalist.list.DataList;
import datalist.list.Database;
import datalist.list.DatabaseBrokenError;
import datalist.stream.DBDataStreamer;
import datalist.stream.DBDataStreamers;
import datalist.stream.FixedSizeDataStreamer;
import datalist.types.SortedIntSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class DataListExample
{
  public static void main(String[] args) throws IOException
  {
    File dbDir = Files.createTempDirectory("DataList-").toFile();
    File idxFile1 = Files.createTempFile("DataList-index-", "").toFile();
    File idxFile2 = Files.createTempFile("DataList-index-", "").toFile();
    idxFile1.deleteOnExit();
    idxFile2.deleteOnExit();
    try
    {
      Database db = new Database(dbDir);
      SortedIndex<Integer> age = new SortedIndex<>(idxFile1, DBDataStreamers.ints_no_nulls);
      SetIndex<Gender> gender = new SetIndex<>(idxFile2, GenderStreamer.INSTANCE);
      DataList<Person> list = new DataList<>(db, "persons", new PersonStreamer())
        .withIndex(age, p->p.getAge(), false)
        .withIndex(gender, p->p.getGender(), false);

      //add data, update index once
      list.addAll(Arrays.asList(
        new Person("James", 5, Gender.MALE),
        new Person("Sandra", 15, Gender.FEMALE),
        new Person("Lisa", 25, Gender.FEMALE),
        new Person("Michael", 22, Gender.MALE),
        new Person("Maria", 3, Gender.MALE),
        new Person("David", 77, Gender.MALE),
        new Person("Susan", 43, Gender.FEMALE),
        new Person("Donald", 37, Gender.MALE),
        new Person("Charles", 27, Gender.MALE)
      ));

      //filter data
      System.out.println("Males:");
      final SortedIntSet malesIndexes = list.where(age, 15, 45).and(gender, Gender.MALE).getIndexes();
      list.sublist(malesIndexes).forEach(System.out::println);

      System.out.printf("%nFemales:%n");
      final SortedIntSet femaleIndexes = list.where(age, 15, 45).and(gender, Gender.FEMALE).getIndexes();
      list.sublist(femaleIndexes).forEach(System.out::println);
      
      SortedIntSet other = SortedIntSet.allValues(list.size());
      other.removeAll(malesIndexes);
      other.removeAll(femaleIndexes);
      System.out.printf("%nSkiped:%n");
      list.sublist(other).forEach(System.out::println);
    }
    finally
    {
      deleteRecursive(dbDir);
    }
  }

  private static boolean deleteRecursive(File path)
  {
    if (path.isDirectory()) {
        for (File file : path.listFiles()) {
            if (!deleteRecursive(file))
                return false;
        } 
    }
    return path.delete();
  }

  private static enum Gender {MALE, FEMALE}

  private static class Person
  {
    private final String name;
    private final int age;
    private final Gender gender;

    public Person(String name, int age, Gender gender)
    {
      this.name = name;
      this.age = age;
      this.gender = gender;
    }

    public String getName()
    {
      return name;
    }

    public int getAge()
    {
      return age;
    }

    public Gender getGender()
    {
      return gender;
    }

    @Override
    public String toString()
    {
      return name + ", " + age + " years old";
    }
  }
  
  private static class PersonStreamer implements DBDataStreamer<Person>
  {
    @Override
    public Person read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      return new Person(buffer.getString(), buffer.getInt(), GenderStreamer.INSTANCE.read(buffer));
    }

    @Override
    public void write(WriteBuffer buffer, Person item)
    {
      buffer.putString(item.getName());
      buffer.putInt(item.getAge());
      GenderStreamer.INSTANCE.write(buffer, item.getGender());
    }
  }

  private static class GenderStreamer implements FixedSizeDataStreamer<Gender>
  {
    @Override
    public int itemSize()
    {
      return 1;
    }

    @Override
    public Gender read(ReadBuffer buffer) throws DatabaseBrokenError
    {
      switch (buffer.getChar())
      {
        case 'm': return Gender.MALE;
        case 'f': return Gender.FEMALE;
        default: throw new DatabaseBrokenError("Unknown gender");
      }
    }

    @Override
    public void write(WriteBuffer buffer, Gender item)
    {
      switch (item)
      {
        case MALE:
          buffer.putChar('m');
          break;
        case FEMALE:
          buffer.putChar('f');
          break;
        default:
          throw new IllegalArgumentException("Unsupported gender: " + item);
      }
    }

    private static GenderStreamer INSTANCE = new GenderStreamer();
  }
}