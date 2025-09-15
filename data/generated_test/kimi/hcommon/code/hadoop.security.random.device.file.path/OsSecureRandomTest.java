package org.apache.hadoop.crypto.random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({OsSecureRandom.class, FileInputStream.class})
public class OsSecureRandomTest {

    @Test
    public void testCustomRandomDeviceFileIsUsedWhenKeyIsSet() throws Exception {
        // 1. Create Configuration and set custom value
        Configuration conf = new Configuration();
        String customPath = "/custom/random";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Compute expected value from Configuration
        String expectedPath = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

        // 3. Prepare mocks
        FileInputStream mockedStream = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockedStream);

        // 4. Invoke method under test
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[16]);

        // 5. Verify FileInputStream was opened with expected path
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(expectedPath));
    }
}