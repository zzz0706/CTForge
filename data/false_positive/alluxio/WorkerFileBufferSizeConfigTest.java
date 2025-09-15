package alluxio.conf;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class WorkerFileBufferSizeConfigTest {

  @Test
  public void testWorkerFileBufferSizeConstraints() {
    // Step 1: Load the Alluxio configuration
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();

    // Step 2: Get the value of the configuration property
    String workerFileBufferSize = conf.get(PropertyKey.WORKER_FILE_BUFFER_SIZE);

    // Step 3: Perform validation on the retrieved configuration value
    /*
     * Validation Criteria:
     * 1. The value should not be null or empty.
     * 2. The value must represent a valid size format (e.g., "1MB", "1024KB", "1048576B").
     * 3. The size must be non-negative integer.
     */
    Assert.assertNotNull("Configuration alluxio.worker.file.buffer.size should not be null", workerFileBufferSize);
    Assert.assertFalse("Configuration alluxio.worker.file.buffer.size should not be empty", workerFileBufferSize.isEmpty());

    try {
      // Parse the value using Alluxio's utility to convert size strings to bytes
      long bufferSizeInBytes = alluxio.util.FormatUtils.parseSpaceSize(workerFileBufferSize);

      // Ensure that the buffer size is positive
      Assert.assertTrue("Configuration alluxio.worker.file.buffer.size must be a non-negative integer ", bufferSizeInBytes >= 0);
    } catch (IllegalArgumentException e) {
      Assert.fail("Configuration alluxio.worker.file.buffer.size must be a valid size format (e.g., \"1MB\", \"1024KB\", \"1048576B\"): " + e.getMessage());
    }
  }
}