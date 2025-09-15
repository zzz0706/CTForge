package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SSLFactory.class, SecurityUtil.class})
public class KMSClientProviderConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void negativeValueThrowsIllegalArgumentException() throws Exception {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();

        // 2. Dynamic expected value calculation
        int negativeValue = -100;
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE, negativeValue);

        // 3. Mock external dependencies
        PowerMockito.mockStatic(SSLFactory.class);
        PowerMockito.mockStatic(SecurityUtil.class);

        SSLFactory mockSslFactory = mock(SSLFactory.class);
        PowerMockito.whenNew(SSLFactory.class).withArguments(
                any(SSLFactory.Mode.class), any(Configuration.class)).thenReturn(mockSslFactory);

        PowerMockito.when(SecurityUtil.buildTokenService(any(URI.class)))
                .thenReturn(mock(Text.class));

        // 4. Invoke method under test
        URI uri = URI.create("kms://http@localhost:9600/kms");
        new KMSClientProvider(uri, conf);
    }
}