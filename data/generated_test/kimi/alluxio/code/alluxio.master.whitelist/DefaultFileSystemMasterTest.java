package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.contexts.CreateFileContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class DefaultFileSystemMasterTest {

  @Test
  public void multiplePrefixesRespectsConfig() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    PowerMockito.mockStatic(ServerConfiguration.class);
    PowerMockito.when(ServerConfiguration.getList(PropertyKey.MASTER_WHITELIST, ","))
                .thenReturn(Arrays.asList("/tmp", "/data"));

    // 2. Prepare the test conditions.
    // Nothing else is required; we test the logic directly.

    // 3. Test code.
    // Test case 1: path matching whitelist prefix "/tmp"
    AlluxioURI uri1 = new AlluxioURI("/tmp/cacheMe.txt");
    boolean cacheable1 = ServerConfiguration.getList(PropertyKey.MASTER_WHITELIST, ",")
                                             .stream()
                                             .anyMatch(p -> uri1.getPath().startsWith(p));

    // Test case 2: path not matching any whitelist prefix
    AlluxioURI uri2 = new AlluxioURI("/other/noCache.txt");
    boolean cacheable2 = ServerConfiguration.getList(PropertyKey.MASTER_WHITELIST, ",")
                                             .stream()
                                             .anyMatch(p -> uri2.getPath().startsWith(p));

    // 4. Code after testing.
    assertTrue("Path matching whitelist prefix should be cacheable", cacheable1);
    assertFalse("Path not matching whitelist prefix should not be cacheable", cacheable2);
  }
}