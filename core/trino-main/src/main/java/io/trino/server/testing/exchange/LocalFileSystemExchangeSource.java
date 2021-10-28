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
package io.trino.server.testing.exchange;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.trino.spi.exchange.ExchangeSource;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSource
        implements ExchangeSource
{
    private static final long BUFFER_SIZE_IN_BYTES = DataSize.of(4, KILOBYTE).toBytes();

    @GuardedBy("this")
    private final Iterator<Path> files;

    @GuardedBy("this")
    private SliceInput sliceInput;
    @GuardedBy("this")
    private boolean closed;

    public LocalFileSystemExchangeSource(List<Path> files)
    {
        this.files = ImmutableList.copyOf(requireNonNull(files, "files is null")).iterator();
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed || (!files.hasNext() && sliceInput == null);
    }

    @Nullable
    @Override
    public synchronized Slice read()
    {
        if (isFinished()) {
            return null;
        }

        if (sliceInput != null && !sliceInput.isReadable()) {
            sliceInput.close();
            sliceInput = null;
        }

        if (sliceInput == null) {
            if (files.hasNext()) {
                Path file = files.next();
                try {
                    sliceInput = new InputStreamSliceInput(new FileInputStream(file.toFile()));
                }
                catch (FileNotFoundException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        if (sliceInput == null) {
            return null;
        }

        if (!sliceInput.isReadable()) {
            sliceInput.close();
            sliceInput = null;
            return null;
        }

        int size = sliceInput.readInt();
        return sliceInput.readSlice(size);
    }

    @Override
    public synchronized long getSystemMemoryUsage()
    {
        return sliceInput != null ? BUFFER_SIZE_IN_BYTES : 0;
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closed = true;
            if (sliceInput != null) {
                sliceInput.close();
                sliceInput = null;
            }
        }
    }
}
