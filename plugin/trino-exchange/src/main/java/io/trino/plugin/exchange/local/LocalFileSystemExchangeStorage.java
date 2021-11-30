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
package io.trino.plugin.exchange.local;

import com.google.common.io.MoreFiles;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import io.trino.spi.TrinoException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.plugin.exchange.FileSystemExchangeManager.AES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;

public class LocalFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        Files.createDirectories(Paths.get(dir.getPath()));
    }

    @Override
    public SliceInput getSliceInput(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        if (secretKey.isPresent()) {
            try {
                final Cipher cipher = Cipher.getInstance(AES);
                cipher.init(Cipher.DECRYPT_MODE, secretKey.get());
                return new InputStreamSliceInput(new CipherInputStream(new FileInputStream(Paths.get(file.getPath()).toFile()), cipher), BUFFER_SIZE_IN_BYTES);
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherInputStream " + e.getMessage(), e);
            }
        }
        else {
            return new InputStreamSliceInput(new FileInputStream(Paths.get(file.getPath()).toFile()), BUFFER_SIZE_IN_BYTES);
        }
    }

    @Override
    public SliceOutput createSliceOutput(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        if (secretKey.isPresent()) {
            try {
                final Cipher cipher = Cipher.getInstance(AES);
                cipher.init(Cipher.ENCRYPT_MODE, secretKey.get());
                return new OutputStreamSliceOutput(new CipherOutputStream(new FileOutputStream(Paths.get(file.getPath()).toFile()), cipher), BUFFER_SIZE_IN_BYTES);
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherOutputStream " + e.getMessage(), e);
            }
        }
        else {
            return new OutputStreamSliceOutput(new FileOutputStream(Paths.get(file.getPath()).toFile()), BUFFER_SIZE_IN_BYTES);
        }
    }

    @Override
    public boolean exists(URI file)
    {
        return Files.exists(Paths.get(file.getPath()));
    }

    @Override
    public void createFile(URI file)
            throws IOException
    {
        Files.createFile(Paths.get(file.getPath()));
    }

    @Override
    public void deleteRecursively(URI dir)
            throws IOException
    {
        MoreFiles.deleteRecursively(Paths.get(dir.getPath()), ALLOW_INSECURE);
    }

    @Override
    public Stream<URI> listFiles(URI dir)
            throws IOException
    {
        return Files.list(Paths.get(dir.getPath())).filter(Files::isRegularFile).map(Path::toUri);
    }

    @Override
    public Stream<URI> listDirectories(URI dir)
            throws IOException
    {
        return Files.list(Paths.get(dir.getPath())).filter(Files::isDirectory).map(Path::toUri);
    }

    @Override
    public long size(URI file)
            throws IOException
    {
        return Files.size(Paths.get(file.getPath()));
    }

    @Override
    public void close()
    {
    }
}
