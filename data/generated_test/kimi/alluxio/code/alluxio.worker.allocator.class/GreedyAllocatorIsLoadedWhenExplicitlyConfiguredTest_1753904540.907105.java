package alluxio.worker.block.allocator;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataView;
import alluxio.util.CommonUtils;

public class GreedyAllocatorIsLoadedWhenExplicitlyConfiguredTest {

  @Before
  public void setUp() {
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void greedyAllocatorIsLoadedWhenExplicitlyConfigured() {
    // 1. Use Alluxio 2.1.0 API to set the configuration
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS,
        "alluxio.worker.block.allocator.GreedyAllocator");

    // 2. Prepare test conditions
    BlockMetadataView mockView = Mockito.mock(BlockMetadataView.class);

    // 3. Test code: invoke the public method that internally uses the configuration
    Allocator allocator = Allocator.Factory.create(mockView);

    // 4. Code after testing
    assertTrue("Expected GreedyAllocator instance",
        allocator instanceof GreedyAllocator);
  }
}