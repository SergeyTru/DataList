package datalist.master_details;

import datalist.types.SortedIntSet;
import java.io.IOException;
import java.nio.file.Files;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CrossIndexTest
{
  // 0 -> 1
  // 1 -> 1
  // 2 -> 2
  // 3 -> 2
  // 4 -> 0
  @Test
  public void testIntersection() throws IOException
  {
    CrossIndex cross = new CrossIndex(Files.createTempFile("cross-", "-index").toFile());
    int[] indexes = new int[] {1, 1, 2, 2, 0};
    cross.add(indexes, indexes.length);

    SortedIntSet mastAll = SortedIntSet.allValues(3);
    SortedIntSet mastOne = new SortedIntSet(new int[]{2});
    SortedIntSet detAll = SortedIntSet.allValues(5);
    SortedIntSet detOne = new SortedIntSet(new int[]{2, 3});

    checkIntersection(cross, mastAll, detAll, mastAll, detAll);
    checkIntersection(cross, mastOne, detOne, mastOne, detOne);
    checkIntersection(cross, mastAll, detOne, mastOne, detOne);
    checkIntersection(cross, mastOne, detAll, mastOne, detOne);
  }

  private void checkIntersection(CrossIndex cross, SortedIntSet initialMaster, SortedIntSet initialDetails, SortedIntSet resultMaster, SortedIntSet resultDetails)
  {
    SortedIntSet master = initialMaster.copy();
    SortedIntSet details = initialDetails.copy();
    cross.intersect(master, details);
    assertEquals(resultMaster, master);
    assertEquals(resultDetails, details);
  }
}
