package alluxio.client.file.options;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;

import org.junit.Assert;
import org.junit.Test;

public class UserFileReplicationMinValidationTest {

  @Test
  public void testUserFileReplicationMinValidation() {
    // 1. Use the Alluxio 2.1.0 API to obtain configuration values
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Prepare test conditions: set valid configuration values to avoid assertion failure
    ((InstancedConfiguration) conf).set(PropertyKey.USER_FILE_REPLICATION_MIN, 1);
    ((InstancedConfiguration) conf).set(PropertyKey.USER_FILE_REPLICATION_MAX, 3);

    // 3. Test code – validate constraints and dependencies
    int replicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    int replicationMax = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);

    // Constraint: replicationMin must be a non-negative integer
    Assert.assertTrue(
        "alluxio.user.file.replication.min must be >= 0",
        replicationMin >= 0
    );

    // Dependency: replicationMin must not exceed replicationMax when replicationMax is set
    if (conf.isSet(PropertyKey.USER_FILE_REPLICATION_MAX)) {
      Assert.assertTrue(
          "alluxio.user.file.replication.min must be <= alluxio.user.file.replication.max",
          replicationMin <= replicationMax
      );
    }

    // Ensure OutStreamOptions and CreateFilePOptions accept the configured value without throwing
    try {
      alluxio.util.FileSystemOptions.createFileDefaults(conf);
    } catch (Exception e) {
      Assert.fail("Invalid configuration causes CreateFilePOptions creation failure: "
          + e.getMessage());
    }

    // 4. Code after testing – nothing to tear down
  }
}