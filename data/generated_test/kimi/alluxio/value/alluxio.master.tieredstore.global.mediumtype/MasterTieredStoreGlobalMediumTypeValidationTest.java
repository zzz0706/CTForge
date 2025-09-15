package alluxio.master.file.meta;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MasterTieredStoreGlobalMediumTypeValidationTest {

  private String mOriginalMediumTypes;

  @Before
  public void before() {
    mOriginalMediumTypes = ServerConfiguration.get(PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE);
  }

  @After
  public void after() {
    ServerConfiguration.set(PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, mOriginalMediumTypes);
  }

  @Test
  public void validateMediumTypeList() {
    // 1. Obtain the current configuration value
    List<String> mediumTypes = ServerConfiguration.getList(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");

    // 2. Allowed medium types according to the default value and usage in InodeTree
    List<String> allowedMediumTypes = Arrays.asList("MEM", "SSD", "HDD");

    // 3. Ensure every configured medium type is valid
    for (String medium : mediumTypes) {
      assertTrue("Configured medium type '" + medium + "' is not valid",
          allowedMediumTypes.contains(medium));
    }
  }

  @Test
  public void validateEmptyMediumType() {
    // 1. Obtain the current configuration value
    List<String> mediumTypes = ServerConfiguration.getList(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");

    // 2. Ensure the list is not empty; an empty list would make pinning impossible
    assertTrue("Medium type list must not be empty", !mediumTypes.isEmpty());
  }

  @Test
  public void validateCaseSensitivity() {
    // 1. Obtain the current configuration value
    List<String> mediumTypes = ServerConfiguration.getList(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");

    // 2. Allowed medium types (case-sensitive)
    List<String> allowedMediumTypes = Arrays.asList("MEM", "SSD", "HDD");

    // 3. Ensure case-sensitive match
    for (String medium : mediumTypes) {
      assertTrue("Medium type '" + medium + "' is not in correct case",
          allowedMediumTypes.contains(medium));
    }
  }
}