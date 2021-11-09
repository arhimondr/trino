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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DistributedFileSystemExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private final int partitionId;
    private final List<Path> files;

    @JsonCreator
    public DistributedFileSystemExchangeSourceHandle(@JsonProperty("partitionId") int partitionId, @JsonProperty("files") List<Path> files)
    {
        this.partitionId = partitionId;
        this.files = ImmutableList.copyOf(requireNonNull(files, "files is null"));
    }

    @Override
    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    @JsonSerialize(using = PathListSerializer.class)
    @JsonDeserialize(using = PathListDeserializer.class)
    public List<Path> getFiles()
    {
        return files;
    }

    public static class PathListSerializer
            extends JsonSerializer<List<Path>>
    {

        @Override
        public void serialize(List<Path> files, JsonGenerator gen, SerializerProvider serializers)
                throws IOException
        {
            gen.writeStartArray();
            for (Path file : files) {
                gen.writeString(file.toString());
            }
            gen.writeEndArray();
        }
    }

    public static class PathListDeserializer
            extends JsonDeserializer<List<Path>>
    {

        @Override
        public List<Path> deserialize(JsonParser jsonParser, DeserializationContext ctxt)
                throws IOException
        {
            List<Path> pathList = new ArrayList<>();

            ObjectCodec oc = jsonParser.getCodec();
            JsonNode node = oc.readTree(jsonParser);
            Iterator<JsonNode> elements = node.elements();
            while (elements.hasNext()) {
                pathList.add(new Path(elements.next().textValue()));
            }
            return pathList;
        }
    }
}
