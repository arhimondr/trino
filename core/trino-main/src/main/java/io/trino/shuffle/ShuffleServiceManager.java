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
package io.trino.shuffle;

import io.airlift.log.Logger;
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.HandleResolver;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.shuffle.ShuffleService;
import io.trino.spi.shuffle.ShuffleServiceFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.SHUFFLE_SERVICE_NOT_CONFIGURED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ShuffleServiceManager
{
    private static final Logger log = Logger.get(EventListenerManager.class);

    private static final File CONFIG_FILE = new File("etc/shuffle-service.properties");
    private static final String SHUFFLE_SERVICE_NAME_PROPERTY = "shuffle-service.name";

    private final HandleResolver handleResolver;

    private final Map<String, ShuffleServiceFactory> shuffleServiceFactories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private ShuffleService shuffleService;

    @Inject
    public ShuffleServiceManager(HandleResolver handleResolver)
    {
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
    }

    public void addShuffleServiceFactory(ShuffleServiceFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (shuffleServiceFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Shuffle service factory '%s' is already registered", factory.getName()));
        }
    }

    public void loadShuffleService()
    {
        if (!CONFIG_FILE.exists()) {
            log.info("Shuffle service configuration file is not present: %s", CONFIG_FILE.getAbsoluteFile());
            return;
        }

        Map<String, String> properties = loadProperties(CONFIG_FILE);
        String name = properties.remove(SHUFFLE_SERVICE_NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name), "Shuffle service configuration %s does not contain %s", CONFIG_FILE, SHUFFLE_SERVICE_NAME_PROPERTY);

        loadShuffleService(name, properties);
    }

    public synchronized void loadShuffleService(String name, Map<String, String> properties)
    {
        log.info("-- Loading shuffle service %s --", name);

        checkState(shuffleService == null, "shuffle service is already loaded");

        ShuffleServiceFactory factory = shuffleServiceFactories.get(name);
        checkArgument(factory != null, "Shuffle service factory '%s' is not registered. Available factories: %s", name, shuffleServiceFactories.keySet());

        ShuffleService shuffleService;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            shuffleService = factory.create(properties);
        }
        handleResolver.setShuffleServiceHandleResolver(factory.getHandleResolver());

        log.info("-- Loaded shuffle service %s --", name);

        this.shuffleService = shuffleService;
    }

    public synchronized ShuffleService getShuffleService()
    {
        if (shuffleService == null) {
            throw new TrinoException(SHUFFLE_SERVICE_NOT_CONFIGURED, "shuffle service is not configured");
        }
        return shuffleService;
    }

    private static Map<String, String> loadProperties(File configFile)
    {
        try {
            return new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }
    }
}
