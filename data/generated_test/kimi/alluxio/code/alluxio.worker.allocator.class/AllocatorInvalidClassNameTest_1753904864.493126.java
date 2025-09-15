package alluxio.worker.block.allocator;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataView;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AllocatorInvalidClassNameTest {

  @Before
  public void setUp() {
    // Ensure a fresh configuration for every test
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    // Clean up after each test
    ServerConfiguration.reset();
  }

  @Test
  public void testInvalidClassNameThrowsRuntimeException() {
    // 1. Prepare the test conditions
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, "non.existing.Allocator");
    BlockMetadataView mockView = Mockito.mock(BlockMetadataView.class);

    // 2. Test code
    try {
      Allocator.Factory.create(mockView);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      // 3. Verify the wrapped cause is ClassNotFoundException
      assertTrue("Expected ClassNotFoundException as cause",
          e.getCause() instanceof ClassNotFoundException);
    }
  }
}