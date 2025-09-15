package alluxio.worker.block.allocator;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMetadataView;
import alluxio.worker.block.allocator.Allocator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class AllocatorTest {
    // Test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testAllocatorCreationWithDefaultConfiguration() {
        // Step 1: Obtain the default configuration value from the Alluxio API
        String allocatorClassName = ServerConfiguration.get(PropertyKey.WORKER_ALLOCATOR_CLASS);

        // Step 2: Simulate the BlockMetadataView with a mock object
        BlockMetadataView mockBlockMetadataView = Mockito.mock(BlockMetadataView.class);

        // Step 3: Create an Allocator instance using the obtained configuration
        Allocator allocator = Allocator.Factory.create(mockBlockMetadataView);

        // Step 4: Validate that the created instance is not null
        Assert.assertNotNull("Allocator instance should not be null", allocator);

        // Step 5: Validate that the created allocator instance matches the expected type
        Assert.assertTrue("Allocator type should match expected",
            allocator.getClass().getName().contains(allocatorClassName));
    }
}