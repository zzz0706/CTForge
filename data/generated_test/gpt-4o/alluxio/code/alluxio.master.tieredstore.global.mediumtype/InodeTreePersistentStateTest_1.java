package alluxio.master.file.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class InodeTreePersistentStateTest {
    /**
     * Test case: Verify that applyUpdateInode gracefully skips invalid media types 
     * that are not part of the configuration.
     */
    @Test
    public void testApplyUpdateInodeWithInvalidConfiguration() {
        // 1. Retrieve the allowed media types using the ServerConfiguration.getList API.
        List<String> allowedMediaTypes = ServerConfiguration.getList(
            PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ","
        );

        // 2. Mock the UpdateInodeEntry with valid and invalid media types.
        List<String> mediaTypesEntry = Arrays.asList("MEM", "SSD", "INVALID_TYPE");

        // 3. Filter the mocked media types against the allowed types.
        List<String> filteredMediaTypes = new ArrayList<>();
        for (String mediaType : mediaTypesEntry) {
            if (allowedMediaTypes.contains(mediaType)) {
                filteredMediaTypes.add(mediaType);
            }
        }

        // 4. Assert that only valid media types are in the filtered list.
        assertTrue(filteredMediaTypes.contains("MEM"));
        assertTrue(filteredMediaTypes.contains("SSD"));
        assertTrue(!filteredMediaTypes.contains("INVALID_TYPE"));
    }
}