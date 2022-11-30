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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class SystemPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private static final SystemPartitioningHandle INSTANCE = new SystemPartitioningHandle();

    @JsonProperty
    public SystemPartitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public SystemPartitionFunction getFunction()
    {
        return function;
    }

    public String getPartitioningName()
    {
        return partitioning.name();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemPartitioningHandle that = (SystemPartitioningHandle) o;
        return partitioning == that.partitioning &&
                function == that.function;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, function);
    }

    @Override
    public String toString()
    {
        if (partitioning == SystemPartitioning.FIXED || partitioning == SystemPartitioning.ARBITRARY) {
            return function.toString();
        }
        return partitioning.toString();
    }

    public PartitionFunction getPartitionFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition, BlockTypeOperators blockTypeOperators)
    {
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        requireNonNull(bucketToPartition, "bucketToPartition is null");

        BucketFunction bucketFunction = function.createBucketFunction(partitionChannelTypes, isHashPrecomputed, bucketToPartition.length, blockTypeOperators);
        return new BucketPartitionFunction(bucketFunction, bucketToPartition);
    }

    public enum SystemPartitionFunction
    {
        HASH {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount, BlockTypeOperators blockTypeOperators)
            {
                if (isHashPrecomputed) {
                    return new HashBucketFunction(new PrecomputedHashGenerator(0), bucketCount);
                }

                return new HashBucketFunction(InterpretedHashGenerator.createPositionalWithTypes(partitionChannelTypes, blockTypeOperators), bucketCount);
            }
        };

        public abstract BucketFunction createBucketFunction(List<Type> partitionChannelTypes,
                boolean isHashPrecomputed,
                int bucketCount,
                BlockTypeOperators blockTypeOperators);
    }
}
