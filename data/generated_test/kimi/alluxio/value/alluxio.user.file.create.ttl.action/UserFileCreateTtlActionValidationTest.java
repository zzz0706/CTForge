package alluxio.conf;

import alluxio.conf.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.TtlAction;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class UserFileCreateTtlActionValidationTest {

  @Test
  public void validateUserFileCreateTtlAction() {
    // 1. Use the Alluxio 2.1.0 API to obtain configuration values.
    AlluxioConfiguration conf = InstancedConfiguration.defaults();

    // 2. Prepare the test conditions: read the configuration value.
    String ttlActionStr = conf.get(PropertyKey.USER_FILE_CREATE_TTL_ACTION);

    // 3. Test code: verify the value is one of the allowed enum constants.
    boolean valid = false;
    for (TtlAction action : TtlAction.values()) {
      if (action.name().equalsIgnoreCase(ttlActionStr)) {
        valid = true;
        break;
      }
    }

    // 4. Code after testing: assert validity.
    assertTrue(
        "Invalid value for " + PropertyKey.USER_FILE_CREATE_TTL_ACTION.getName()
            + ": \"" + ttlActionStr + "\". Allowed values are DELETE and FREE.",
        valid);
  }
}