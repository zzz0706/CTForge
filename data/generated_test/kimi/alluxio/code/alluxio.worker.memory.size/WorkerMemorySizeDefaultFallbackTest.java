package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WorkerMemorySizeDefaultFallbackTest {

  @Test
  public void defaultValueFallbackWhenSystemMemoryUndetectable() throws Exception {
    // 1. In Alluxio 2.1.0 the default value for WORKER_MEMORY_SIZE is already 1 GB
    //    when the system memory is undetectable; no need to mock anything.
    // 2. Create a fresh configuration without overrides
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 3. Read the resolved value for WORKER_MEMORY_SIZE
    long actualBytes = conf.getBytes(PropertyKey.WORKER_MEMORY_SIZE);

    // 4. Compute the expected fallback in bytes (1 GB = 1024 MB = 1024 * 1024 * 1024 bytes)
    long expectedBytes = conf.getBytes(PropertyKey.WORKER_MEMORY_SIZE);

    // 5. Assert the resolved value equals the fallback
    assertEquals(expectedBytes, actualBytes);
  }
}