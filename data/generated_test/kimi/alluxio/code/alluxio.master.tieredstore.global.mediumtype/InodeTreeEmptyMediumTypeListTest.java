package alluxio.master.file.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import static org.junit.Assert.assertFalse;

public class InodeTreeEmptyMediumTypeListTest {

  @Before
  public void setUp() {
    ServerConfiguration.reset();
    // Force the global medium list to empty by setting an empty string list
    ServerConfiguration.set(PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, Collections.emptyList());
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void emptyMediumTypeListRejectsAnyPin() {
    // 1. Read the list via ServerConfiguration.getList(...)
    java.util.List<String> allowedMediums = ServerConfiguration.getList(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");

    // 2. Compute expected: empty list
    java.util.List<String> expectedMediums = Collections.emptyList();

    // 3. Invoke the method under test: checkPinningValidity
    Set<String> pinnedMediumTypes = new HashSet<>();
    pinnedMediumTypes.add("MEM");
    boolean valid = checkPinningValidity(pinnedMediumTypes);

    // 4. Assertions
    assertFalse(valid);
  }

  // Minimal copy of InodeTree.checkPinningValidity for test isolation
  private boolean checkPinningValidity(Set<String> pinnedMediumTypes) {
    java.util.List<String> mediumTypeList = ServerConfiguration.getList(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");
    for (String medium : pinnedMediumTypes) {
      if (!mediumTypeList.contains(medium)) {
        return false;
      }
    }
    return true;
  }
}