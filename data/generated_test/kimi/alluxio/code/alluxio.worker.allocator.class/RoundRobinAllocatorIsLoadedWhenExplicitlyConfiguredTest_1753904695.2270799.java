package alluxio.worker.block.allocator;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMetadataView;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RoundRobinAllocatorIsLoadedWhenExplicitlyConfiguredTest {

    @Test
    public void roundRobinAllocatorIsLoadedWhenExplicitlyConfigured() {
        // 1. Use Alluxio 2.1.0 API to set configuration
        ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS,
                "alluxio.worker.block.allocator.RoundRobinAllocator");

        // 2. Prepare test prerequisites
        BlockMetadataView mockBlockMetadataView = mock(BlockMetadataView.class);

        // 3. Test code – exercise the public API that internally uses ServerConfiguration
        Allocator allocator = Allocator.Factory.create(mockBlockMetadataView);

        // 4. Assertion
        assertTrue("Expected RoundRobinAllocator instance",
                allocator instanceof RoundRobinAllocator);
    }

    @After
    public void tearDown() {
        ServerConfiguration.reset();
    }
}