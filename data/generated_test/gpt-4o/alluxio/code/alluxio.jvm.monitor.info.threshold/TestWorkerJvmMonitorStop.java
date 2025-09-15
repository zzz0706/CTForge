package alluxio.worker;   

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.AlluxioWorkerProcess;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestWorkerJvmMonitorStop {

    @Test
    public void testWorkerJvmMonitorStop() throws Exception {
        // Step 1: Verify if the WORKER_JVM_MONITOR_ENABLED property is true
        boolean isJvmMonitorEnabled = ServerConfiguration.getBoolean(PropertyKey.WORKER_JVM_MONITOR_ENABLED);
        if (!isJvmMonitorEnabled) {
            // Skip the test if JVM Monitor is disabled
            return;
        }

        // Step 2: Create a mock TieredIdentity using correct API for the AlluxioWorkerProcess constructor
        TieredIdentity tieredIdentity = new TieredIdentity(Collections.singletonList(new LocalityTier("testTier", "testValue")));

        // Step 3: Instantiate and start the worker process
        AlluxioWorkerProcess workerProcess = new AlluxioWorkerProcess(tieredIdentity);
        workerProcess.start();

        // Step 4: Use reflection to access the private isServing() method
        java.lang.reflect.Method isServingMethod = workerProcess.getClass().getDeclaredMethod("isServing");
        isServingMethod.setAccessible(true);
        boolean isServing = (Boolean) isServingMethod.invoke(workerProcess);

        // Verify the worker has started successfully
        Assert.assertTrue("Worker process should be serving.", isServing);

        // Step 5: Stop the worker process
        workerProcess.stop();

        // Step 6: Validate that the worker process is not serving anymore
        isServing = (Boolean) isServingMethod.invoke(workerProcess);
        Assert.assertFalse("Worker process should not be serving.", isServing);
    }
}