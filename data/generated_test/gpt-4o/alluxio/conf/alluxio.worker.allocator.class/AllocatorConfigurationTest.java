package alluxio.worker.block.allocator;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class AllocatorConfigurationTest {

    @Test
    public void testWorkerAllocatorClassConfigValidity() {
        // Step 1: Fetch the configuration value of "alluxio.worker.allocator.class" using Alluxio Configuration API.
        AlluxioConfiguration configuration = ServerConfiguration.global();
        String workerAllocatorClass = configuration.get(PropertyKey.WORKER_ALLOCATOR_CLASS);

        // Step 2: Validate the configuration value against the allowed constraints.
        // Allowed values: MaxFreeAllocator, GreedyAllocator, RoundRobinAllocator
        boolean isValid = workerAllocatorClass.equals("alluxio.worker.block.allocator.MaxFreeAllocator")
            || workerAllocatorClass.equals("alluxio.worker.block.allocator.GreedyAllocator")
            || workerAllocatorClass.equals("alluxio.worker.block.allocator.RoundRobinAllocator");

        // Step 3: Assert that the configuration value is valid. 
        Assert.assertTrue("Invalid configuration for alluxio.worker.allocator.class: " + workerAllocatorClass, isValid);
    }
}