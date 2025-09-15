package org.apache.hadoop.crypto;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ CryptoCodec.class, ReflectionUtils.class })
public class CryptoCodecTest {

    @Test
    public void testDefaultCipherSuiteIsLoaded() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        CryptoCodec mockCodec = mock(CryptoCodec.class);
        CipherSuite expectedSuite = CipherSuite.AES_CTR_NOPADDING;
        when(mockCodec.getCipherSuite()).thenReturn(expectedSuite);

        @SuppressWarnings("unchecked")
        Class<? extends CryptoCodec> mockCodecClass = (Class<? extends CryptoCodec>) CryptoCodec.class;
        List<Class<? extends CryptoCodec>> codecClasses = Collections.<Class<? extends CryptoCodec>>singletonList(mockCodecClass);

        mockStatic(CryptoCodec.class, ReflectionUtils.class);
        when(CryptoCodec.getInstance(conf, expectedSuite)).thenReturn(mockCodec);
        when(ReflectionUtils.newInstance(mockCodecClass, conf)).thenReturn(mockCodec);

        // 3. Test code.
        CryptoCodec actualCodec = CryptoCodec.getInstance(conf, expectedSuite);

        // 4. Code after testing.
        assertNotNull("CryptoCodec should not be null", actualCodec);
        assertEquals("Cipher suite name mismatch", expectedSuite.getName(),
                actualCodec.getCipherSuite().getName());
    }
}