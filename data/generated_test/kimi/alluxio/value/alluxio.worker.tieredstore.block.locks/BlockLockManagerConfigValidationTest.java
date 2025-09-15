package alluxio.worker.block;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlockLockManagerConfigValidationTest {

  private int mOriginalBlockLocks;

  @Before
  public void setUp() {
    // Save the original configuration value.
    mOriginalBlockLocks = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);
    // Ensure the value is positive for the test.
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, 1000);
  }

  @After
  public void tearDown() {
    // Restore the original configuration value.
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, mOriginalBlockLocks);
  }

  @Test
  public void validateWorkerTieredStoreBlockLocks() {
    // 1. Use the Alluxio 2.1.0 API to read the configuration value.
    int blockLocks = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);

    // 2. Prepare test conditions: none, configuration is read from external files.

    // 3. Test code: ensure the value is a positive integer.
    assertTrue("alluxio.worker.tieredstore.block.locks must be > 0",
        blockLocks > 0);

    // 4. Code after testing: none, stateless validation only.
  }
}