package alluxio.worker.block.allocator;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataView;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
  public void greedyAllocatorIsLoadedWhenExplicitlyConfigured() throws Exception {
    // 1. Use Alluxio 2.1.0 API to set the configuration
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS,
        "alluxio.worker.block.allocator.GreedyAllocator");

    // 2. Prepare test conditions
    BlockMetadataView mockView = Mockito.mock(BlockMetadataView.class);

    // 3. Test code: invoke the public method that internally uses the configuration
    Allocator allocator = Allocator.Factory.create(mockView);

    // 4. Assertions
    assertTrue("Expected GreedyAllocator instance",
        allocator instanceof GreedyAllocator);

    // Additional coverage: ensure CommonUtils.createNewClassInstance is exercised
    Class<Allocator> allocClass = ServerConfiguration.getClass(PropertyKey.WORKER_ALLOCATOR_CLASS);
    Allocator viaCommonUtils = CommonUtils.createNewClassInstance(
        allocClass,
        new Class[] { BlockMetadataView.class },
        new Object[] { mockView });
    assertTrue("Expected GreedyAllocator via CommonUtils",
        viaCommonUtils instanceof GreedyAllocator);
  }
}