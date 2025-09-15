package alluxio.worker.block.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class WorkerDataFolderTmpValidationTest {

  private static final PropertyKey KEY = PropertyKey.WORKER_DATA_TMP_FOLDER;
  private static final PropertyKey TIER_PATH_KEY =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0);
  private static final PropertyKey TIER_QUOTA_KEY =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(0);
  private static final PropertyKey TIER_MEDIUM_KEY =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE.format(0);

  @Before
  public void setUp() {
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  /**
   * Validates that the configured value for {@code alluxio.worker.data.folder.tmp} is a
   * non-empty, relative path segment and does not contain path separators.
   */
  @Test
  public void validateTmpFolderPath() {
    // 1. Load configuration from external files (not set in test code)
    String tmpFolder = ServerConfiguration.get(KEY);

    // 2. Prepare minimal tier configuration to satisfy StorageTier initialization
    ServerConfiguration.set(TIER_PATH_KEY, "/tmp/alluxio");
    ServerConfiguration.set(TIER_QUOTA_KEY, "1GB");
    ServerConfiguration.set(TIER_MEDIUM_KEY, "MEM");

    // 3. Validate constraints
    assertTrue("alluxio.worker.data.folder.tmp must not be null or empty",
        tmpFolder != null && !tmpFolder.trim().isEmpty());

    // Ensure the path is relative and contains no separators
    assertFalse("alluxio.worker.data.folder.tmp must be a relative path without separators",
        tmpFolder.contains("/"));

    // Ensure the path does not start with a separator
    assertFalse("alluxio.worker.data.folder.tmp must not start with a separator",
        tmpFolder.startsWith("/"));

    // 4. Clean up (handled by tearDown)
  }
}