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
package io.trino.plugin.exchange;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class FileSystemExchangeQueryRunner
{
    private static final Session DEFAULT_SESSION = testSessionBuilder()
            .setSource("test")
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private FileSystemExchangeQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(DEFAULT_SESSION)
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("sql.default-catalog", "tpch")
                        .put("sql.default-schema", "tiny")
                        .put("retry-policy", "TASK")
                        .put("query.initial-hash-partitions", "5")
                        .put("fault-tolerant-execution-target-task-input-size", "10MB")
                        .put("fault-tolerant-execution-target-task-split-count", "4")
                        // TODO: re-enable once failure recover supported for this functionality
                        .put("enable-dynamic-filtering", "false")
                        .put("distributed-sort", "false")
                        .build())
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new ExchangePlugin());
        queryRunner.loadExchangeManager("distributed", loadPropertiesFrom("etc/exchange-manager.properties"));
        Thread.sleep(10);
        Logger log = Logger.get(FileSystemExchangeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
