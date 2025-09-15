package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.WorkerNetAddress;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BaseFileSystemTest {

    @Test
    public void testNoFallbackForEmptyLocations() throws IOException, FileDoesNotExistException, AlluxioException {
        // Prepare mock objects and test conditions
        BaseFileSystem baseFileSystem = Mockito.mock(BaseFileSystem.class);
        AlluxioURI testPath = new AlluxioURI("/testPath");

        // Mock getStatus() to return a mocked URIStatus object
        URIStatus mockStatus = Mockito.mock(URIStatus.class);
        Mockito.when(baseFileSystem.getStatus(testPath)).thenReturn(mockStatus);

        // Prepare a list with one block location having no valid locations
        List<BlockLocationInfo> emptyBlockLocations = new ArrayList<>();
        Mockito.when(baseFileSystem.getBlockLocations(testPath)).thenReturn(emptyBlockLocations);

        // Mock the configuration value retrieval via the mock BaseFileSystem
        AlluxioConfiguration mockConfiguration = Mockito.mock(AlluxioConfiguration.class);
        Mockito.when(mockConfiguration.getBoolean(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED)).thenReturn(false);
        Mockito.when(baseFileSystem.getConf()).thenReturn(mockConfiguration);

        // Invoke the method under test
        List<BlockLocationInfo> blockLocations = baseFileSystem.getBlockLocations(testPath);

        // Assert the results
        Assert.assertTrue("BlockLocations list should be empty when fallback is disabled", blockLocations.isEmpty());
    }
}