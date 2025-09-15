package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.InvalidPathException;
import java.nio.file.Paths;

public class MasterBackupDirectoryConfigTest {

  /**
   * Test to validate the configuration `alluxio.master.backup.directory`.
   * This test checks to ensure that the configuration value adheres to specified constraints
   * and that any dependencies are correctly handled.
   */
  @Test
  public void testMasterBackupDirectoryConfig() {
    // Step 1: Read the configuration value using Alluxio 2.1.0 APIs.
    String backupDir = ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);

    // Step 2: Validate the constraints of the configuration value.
    try {
      // Check that the path is valid.
      Paths.get(backupDir);
    } catch (InvalidPathException e) {
      Assert.fail("Configuration 'alluxio.master.backup.directory' contains an invalid path: " + backupDir);
    }

    // Step 3: Ensure that the configuration value is specified (not null or empty).
    Assert.assertNotNull("Configuration 'alluxio.master.backup.directory' must not be null.", backupDir);
    Assert.assertFalse("Configuration 'alluxio.master.backup.directory' must not be an empty string.", backupDir.isEmpty());

    // Step 4: Validate dependency constraints (if any).
    String rootUfs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Assert.assertNotNull("Configuration 'alluxio.master.mount.table.root.ufs' must not be null.", rootUfs);
    Assert.assertFalse("Configuration 'alluxio.master.mount.table.root.ufs' must not be empty.", rootUfs.isEmpty());

    // Adjust the validation logic for supported UFS types.
    // Since "/opt/alluxio/underFSStorage" is allowed by design in Alluxio as local FS, add it to valid types.
    if (!rootUfs.startsWith("file://") && !rootUfs.startsWith("hdfs://") && !rootUfs.startsWith("/")) {
      Assert.fail("Configuration 'alluxio.master.mount.table.root.ufs' has an unsupported UFS type: " + rootUfs);
    }

    // Ensure the backup directory resolves correctly within a UFS context.
    try {
      Paths.get(rootUfs, backupDir);
    } catch (InvalidPathException e) {
      Assert.fail("Backup directory configuration resolves to an invalid path within UFS context: "
          + rootUfs + "/" + backupDir);
    }

    // Configuration values passed all validation checks.
    System.out.println("Configuration 'alluxio.master.backup.directory' validated successfully: " + backupDir);
  }
}