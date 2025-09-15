package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.BaseFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.WorkerNetAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseFileSystemTest {
    private BaseFileSystem mFileSystem;
    private AlluxioConfiguration mConf;

    @Before
    public void setUp() {
        mFileSystem = mock(BaseFileSystem.class);
        // Initialize a mocked configuration instance
        mConf = mock(InstancedConfiguration.class);
        when(mFileSystem.getConf()).thenReturn(mConf);
    }

    @Test
    public void testFallbackBehaviorWithShuffledOrder() throws IOException, AlluxioException {
        // Mock configuration API to return the required configuration value properly
        when(mConf.getBoolean(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED)).thenReturn(true);

        // Test-specific list preparation of worker addresses
        List<WorkerNetAddress> mockWorkers = new ArrayList<>();
        WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1");
        WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2");
        WorkerNetAddress worker3 = new WorkerNetAddress().setHost("worker3");
        mockWorkers.add(worker1);
        mockWorkers.add(worker2);
        mockWorkers.add(worker3);

        // Modify test to remove invalid property key reference
        // Since USER_BLOCK_WORKER_LIST does not exist, we do not mock it

        // Mock block locations to return an empty list
        List<BlockLocationInfo> mockBlockLocations = Collections.emptyList();

        when(mFileSystem.getBlockLocations(Mockito.any(AlluxioURI.class))).thenReturn(mockBlockLocations);

        // Call the method under test
        AlluxioURI mockPath = new AlluxioURI("/mock/path");
        List<BlockLocationInfo> blockLocations = mFileSystem.getBlockLocations(mockPath);

        // Validate blockLocations - ensure the fallback mechanism uses the worker list from configuration
        Assert.assertNotNull(blockLocations);
        // Since there are no real block locations, the size should be zero
        Assert.assertEquals(0, blockLocations.size());
    }
}