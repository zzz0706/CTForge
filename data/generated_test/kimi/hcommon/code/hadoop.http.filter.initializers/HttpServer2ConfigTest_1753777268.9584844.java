package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;

import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class})
public class HttpServer2ConfigTest {

    @Test
    public void testEmptyPropertyResultsInNoInitializers() throws Exception {
        // 1. Use HDFS 2.8.5 API to obtain configuration value
        Configuration conf = new Configuration(false);

        // 2. Prepare test condition: set empty string
        conf.set("hadoop.http.filter.initializers", "");

        // 3. Mock static ReflectionUtils to capture instantiation
        mockStatic(ReflectionUtils.class);

        // 4. Build HttpServer2 instance (entry point)
        HttpServer2 server = new HttpServer2.Builder()
                .setName("test")
                .setConf(conf)
                .addEndpoint(URI.create("http://localhost:0"))
                .build();

        // 5. Verify no instantiation and no initFilter calls
        verifyStatic(never());
        ReflectionUtils.newInstance(any(Class.class), any(Configuration.class));

        // 6. Code after testing
        server.stop();
    }
}