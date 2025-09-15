package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WorkerFileBufferSizeParsingTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void unitParsingWorksForKB() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    ServerConfiguration.set(PropertyKey.WORKER_FILE_BUFFER_SIZE, "512KB");

    // 3. Test code.
    long actualBytes = ServerConfiguration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);
    long expectedBytes = 512L * 1024L;

    // 4. Code after testing.
    assertEquals(expectedBytes, actualBytes);
  }
}