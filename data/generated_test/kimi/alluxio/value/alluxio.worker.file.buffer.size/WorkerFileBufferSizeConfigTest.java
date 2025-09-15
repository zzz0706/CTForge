package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for validating the configuration {@link PropertyKey#WORKER_FILE_BUFFER_SIZE}.
 */
public class WorkerFileBufferSizeConfigTest {

  private static final PropertyKey BUFFER_SIZE_KEY = PropertyKey.WORKER_FILE_BUFFER_SIZE;

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  /**
   * Tests that the configuration value is a positive byte size.
   */
  @Test
  public void validatePositiveByteSize() {
    long bufferSize = ServerConfiguration.getBytes(BUFFER_SIZE_KEY);
    Assert.assertTrue("alluxio.worker.file.buffer.size must be > 0", bufferSize > 0);
  }

  /**
   * Tests that the configuration value does not exceed Integer.MAX_VALUE to prevent overflow
   * when cast to int in {@link UnderFileSystemBlockReader} and write handlers.
   */
  @Test
  public void validateMaxIntLimit() {
    long bufferSize = ServerConfiguration.getBytes(BUFFER_SIZE_KEY);
    Assert.assertTrue("alluxio.worker.file.buffer.size must be <= Integer.MAX_VALUE",
        bufferSize <= Integer.MAX_VALUE);
  }
}