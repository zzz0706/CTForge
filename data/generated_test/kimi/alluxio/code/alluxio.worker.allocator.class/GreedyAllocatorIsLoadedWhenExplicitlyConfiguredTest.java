package alluxio.worker.block.allocator;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMetadataView;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    // 1. Read configuration value via Alluxio 2.1.0 API
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS,
        "alluxio.worker.block.allocator.GreedyAllocator");

    // 2. Prepare test conditions
    BlockMetadataView mockView = mock(BlockMetadataView.class);

    // 3. Invoke method under test
    Allocator allocator = Allocator.Factory.create(mockView);

    // 4. Assertions and verification
    assertTrue("Expected GreedyAllocator instance",
        allocator instanceof alluxio.worker.block.allocator.GreedyAllocator);
  }
}