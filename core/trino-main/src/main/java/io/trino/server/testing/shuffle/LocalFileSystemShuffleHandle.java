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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.shuffle.ShuffleHandle;

import static java.util.Objects.requireNonNull;

public class LocalFileSystemShuffleHandle
        implements ShuffleHandle
{
    private final String shuffleId;
    private final int outputPartitionCount;

    @JsonCreator
    public LocalFileSystemShuffleHandle(@JsonProperty("shuffleId") String shuffleId, @JsonProperty("outputPartitionCount") int outputPartitionCount)
    {
        this.shuffleId = requireNonNull(shuffleId, "shuffleId is null");
        this.outputPartitionCount = outputPartitionCount;
    }

    @JsonProperty
    public String getShuffleId()
    {
        return shuffleId;
    }

    @JsonProperty
    public int getOutputPartitionCount()
    {
        return outputPartitionCount;
    }
}
