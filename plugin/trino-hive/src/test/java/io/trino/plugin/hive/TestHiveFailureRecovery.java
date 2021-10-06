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
package io.trino.plugin.hive;

import io.trino.Session;
import io.trino.testing.AbstractTestFailureRecovery;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TestHiveFailureRecovery
        extends AbstractTestFailureRecovery
{
    @Override
    protected QueryRunner createQueryRunner(List<TpchTable<?>> requiredTpchTables, Map<String, String> configProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(requiredTpchTables)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(configProperties)
                .build();
    }

    @Override
    @Test(enabled = false)
    // create table is not atomic at the moment
    public void testCreateTable()
    {
        super.testCreateTable();
    }

    @Override
    @Test(enabled = false)
    // delete is unsupported for non ACID tables
    public void testDelete()
    {
        super.testDelete();
    }

    @Override
    @Test(enabled = false)
    // delete is unsupported for non ACID tables
    public void testDeleteWithSubquery()
    {
        super.testDeleteWithSubquery();
    }

    @Override
    @Test(enabled = false)
    // update is unsupported for non ACID tables
    public void testUpdate()
    {
        super.testUpdate();
    }

    @Override
    @Test(enabled = false)
    // update is unsupported for non ACID tables
    public void testUpdateWithSubquery()
    {
        super.testUpdateWithSubquery();
    }

    @Override
    @Test(enabled = false)
    // materialized views are currently not implemented by Hive connector
    public void testRefreshMaterializedView()
    {
        super.testRefreshMaterializedView();
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // create table is not atomic at the moment
    public void testCreatePartitionedTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // create partition is not atomic at the moment
    public void testInsertIntoNewPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoExistingPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // replace partition is not atomic at the moment
    public void testReplaceExistingPartition()
    {
        testTableModification(
                Optional.of(Session.builder(getQueryRunner().getDefaultSession())
                        .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                        .build()),
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // delete is unsupported for non ACID tables
    public void testDeletePartitionWithSubquery()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 0 p FROM orders"),
                "DELETE FROM <table> WHERE p = (SELECT min(nationkey) FROM nation)",
                Optional.of("DROP TABLE <table>"));
    }
}
