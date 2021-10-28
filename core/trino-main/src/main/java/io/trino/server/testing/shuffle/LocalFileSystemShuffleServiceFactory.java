/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.testing.shuffle;

import io.trino.spi.shuffle.ShuffleHandle;
import io.trino.spi.shuffle.ShufflePartitionHandle;
import io.trino.spi.shuffle.ShuffleService;
import io.trino.spi.shuffle.ShuffleServiceFactory;
import io.trino.spi.shuffle.ShuffleServiceHandleResolver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class LocalFileSystemShuffleServiceFactory
        implements ShuffleServiceFactory
{
    private static final String BASE_DIRECTORY_PROPERTY = "base-directory";

    @Override
    public String getName()
    {
        return "local";
    }

    @Override
    public ShuffleService create(Map<String, String> config)
    {
        String configuredBaseDirectory = config.get(BASE_DIRECTORY_PROPERTY);
        Path baseDirectory;
        if (configuredBaseDirectory != null) {
            baseDirectory = Paths.get(configuredBaseDirectory);
        }
        else {
            try {
                baseDirectory = Files.createTempDirectory("shuffle-service-");
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new LocalFileSystemShuffleService(baseDirectory);
    }

    @Override
    public ShuffleServiceHandleResolver getHandleResolver()
    {
        return new ShuffleServiceHandleResolver()
        {
            @Override
            public Class<? extends ShuffleHandle> getShuffleHandleClass()
            {
                return LocalFileSystemShuffleHandle.class;
            }

            @Override
            public Class<? extends ShufflePartitionHandle> getShufflePartitionHandleClass()
            {
                return LocalFileSystemShufflePartitionHandle.class;
            }
        };
    }
}
