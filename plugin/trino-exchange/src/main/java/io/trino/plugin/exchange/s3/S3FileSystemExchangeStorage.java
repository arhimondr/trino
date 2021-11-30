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
package io.trino.plugin.exchange.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.io.Closer;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import org.openjdk.jol.info.ClassLayout;

import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.amazonaws.regions.Regions.US_EAST_1;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.md5;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.exchange.FileSystemExchangeManager.PATH_SEPARATOR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class S3FileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final String DIRECTORY_SUFFIX = "_$folder$";

    private final boolean pinS3ClientToCurrentRegion;
    private final String endPoint;
    private final AmazonS3 s3;
    private final int streamingUploadPartSize;
    private final ExecutorService uploadExecutor = newCachedThreadPool(threadsNamed("s3-upload-%s"));

    @Inject
    public S3FileSystemExchangeStorage(ExchangeS3Config config)
    {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxErrorRetry(config.getS3MaxErrorRetries())
                .withConnectionTimeout(toIntExact(config.getS3ConnectTimeout().toMillis()))
                .withSocketTimeout(toIntExact(config.getS3SocketTimeout().toMillis()))
                .withMaxConnections(config.getS3MaxConnections())
                .withUserAgentPrefix("")
                .withUserAgentSuffix("Trino-exchange");

        this.pinS3ClientToCurrentRegion = config.isPinS3ClientToCurrentRegion();
        this.endPoint = config.getS3Endpoint();
        verify(!pinS3ClientToCurrentRegion || endPoint == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");
        this.streamingUploadPartSize = toIntExact(config.getS3StreamingPartSize().toBytes());

        this.s3 = createAmazonS3Client(clientConfig, createAwsCredentialsProvider(config));
    }

    @Override
    public void createDirectories(URI dir)
    {
        // no need to do anything for S3
    }

    @Override
    public SliceInput getSliceInput(URI file, Optional<SecretKey> secretKey)
    {
        GetObjectRequest request = new GetObjectRequest(getBucketName(file), keyFromUri(file));
        secretKey.map(SSECustomerKey::new).ifPresent(request::withSSECustomerKey);
        return new InputStreamSliceInput(s3.getObject(request).getObjectContent()); // TODO: decide if we want to make input buffer size configurable
    }

    @Override
    public SliceOutput createSliceOutput(URI file, Optional<SecretKey> secretKey)
    {
        String bucketName = getBucketName(file);
        String key = keyFromUri(file);

        final Optional<SSECustomerKey> sseCustomerKey = secretKey.map(SSECustomerKey::new);

        Supplier<String> uploadIdFactory = () -> initMultipartUpload(bucketName, key, sseCustomerKey).getUploadId();
        return new S3ExchangeSliceOutput(s3, bucketName, key, uploadIdFactory, uploadExecutor, streamingUploadPartSize, sseCustomerKey);
    }

    private InitiateMultipartUploadResult initMultipartUpload(String bucket, String key, Optional<SSECustomerKey> sseCustomerKey)
    {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key)
                .withObjectMetadata(new ObjectMetadata())
                .withCannedACL(CannedAccessControlList.Private);
        sseCustomerKey.ifPresent(request::withSSECustomerKey);

        return s3.initiateMultipartUpload(request);
    }

    @Override
    public boolean exists(URI file)
    {
        return getS3ObjectMetadata(file) != null;
    }

    @Override
    public void createFile(URI file)
    {
        s3.putObject(getBucketName(file), keyFromUri(file), "");
    }

    @Override
    public void deleteRecursively(URI uri)
    {
        if (isDirectory(uri)) {
            listDirectories(uri).forEach(this::deleteRecursively);
            listFiles(uri).forEach(file -> s3.deleteObject(getBucketName(file), keyFromUri(file)));
            s3.deleteObject(getBucketName(uri), keyFromUri(uri) + DIRECTORY_SUFFIX);
        }
        else {
            s3.deleteObject(getBucketName(uri), keyFromUri(uri));
        }
    }

    @Override
    public Stream<URI> listDirectories(URI dir)
    {
        List<String> prefixes = listS3Directory(dir).getCommonPrefixes();
        return prefixes.stream().map(prefix -> {
            try {
                return new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + prefix, dir.getFragment());
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    @Override
    public Stream<URI> listFiles(URI dir)
    {
        List<S3ObjectSummary> objects = listS3Directory(dir).getObjectSummaries();
        return objects.stream().filter(object -> !object.getKey().endsWith(PATH_SEPARATOR)).map(object -> {
            try {
                return new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + object.getKey(), dir.getFragment());
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    @Override
    public long size(URI uri)
            throws IOException
    {
        checkArgument(!isDirectory(uri), "expected a file URI but got a directory URI");
        ObjectMetadata metadata = getS3ObjectMetadata(uri);
        if (metadata == null) {
            throw new FileNotFoundException("File does not exist: " + uri);
        }
        return metadata.getContentLength();
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(uploadExecutor::shutdown);
            closer.register(s3::shutdown);
        }
    }

    private ObjectMetadata getS3ObjectMetadata(URI uri)
    {
        return s3.getObjectMetadata(getBucketName(uri), keyFromUri(uri));
    }

    private ListObjectsV2Result listS3Directory(URI dir)
    {
        String key = keyFromUri(dir);
        if (!key.isEmpty()) {
            key += PATH_SEPARATOR;
        }

        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(getBucketName(dir))
                .withPrefix(key)
                .withDelimiter(PATH_SEPARATOR);
        return s3.listObjectsV2(request);
    }

    private static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    private static String getBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    private AmazonS3 createAmazonS3Client(ClientConfiguration clientConfig, AWSCredentialsProvider credentialsProvider)
    {
        AmazonS3Builder<AmazonS3ClientBuilder, AmazonS3> clientBuilder = AmazonS3Client.builder()
                .withCredentials(credentialsProvider)
                .withClientConfiguration(clientConfig);

        boolean regionOrEndpointSet = false;

        if (pinS3ClientToCurrentRegion) {
            clientBuilder.setRegion(Regions.getCurrentRegion().getName());
            regionOrEndpointSet = true;
        }

        if (endPoint != null) {
            clientBuilder.setEndpointConfiguration(new EndpointConfiguration(endPoint, null));
            regionOrEndpointSet = true;
        }

        if (!regionOrEndpointSet) {
            clientBuilder.withRegion(US_EAST_1);
            clientBuilder.setForceGlobalBucketAccessEnabled(true);
        }

        return clientBuilder.build();
    }

    private static AWSCredentialsProvider createAwsCredentialsProvider(ExchangeS3Config config)
    {
        if (config.getS3AwsAccessKey() != null && config.getS3AwsSecretKey() != null) {
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getS3AwsAccessKey(), config.getS3AwsSecretKey()));
        }
        if (config.getS3IamRole() != null) {
            return new STSAssumeRoleSessionCredentialsProvider
                    .Builder(config.getS3IamRole(), "trino-session")
                    .withExternalId(config.getS3ExternalId())
                    .build();
        }
        return DefaultAWSCredentialsProviderChain.getInstance();
    }

    private static boolean isDirectory(URI uri)
    {
        return uri.toString().endsWith(PATH_SEPARATOR);
    }

    private static class S3ExchangeSliceOutput
            extends SliceOutput
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(S3ExchangeSliceOutput.class).instanceSize();

        private final AmazonS3 s3;
        private final String bucketName;
        private final String key;
        private final Supplier<String> uploadIdFactory;
        private final ExecutorService uploadExecutor;
        private final Optional<SSECustomerKey> sseCustomerKey;

        private int currentPartNumber;
        private byte[] buffer;
        private int bufferSize;

        private boolean failed;
        private Optional<String> uploadId = Optional.empty();
        private Future<UploadPartResult> inProgressUploadFuture;
        private final List<UploadPartResult> parts = new ArrayList<>();

        // TODO: implement async upload (aka. double buffering)
        public S3ExchangeSliceOutput(
                AmazonS3 s3,
                String bucketName,
                String key,
                Supplier<String> uploadIdFactory,
                ExecutorService uploadExecutor,
                int partSize,
                Optional<SSECustomerKey> sseCustomerKey)
        {
            this.s3 = requireNonNull(s3, "s3 is null");

            this.buffer = new byte[partSize];

            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.uploadIdFactory = requireNonNull(uploadIdFactory, "uploadIdFactory is null");
            this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
            this.sseCustomerKey = requireNonNull(sseCustomerKey, "sseCustomerKey is null");
        }

        @Override
        public void writeInt(int value)
        {
            writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
        }

        @Override
        public void writeBytes(Slice slice)
        {
            writeBytes(slice.getBytes());
        }

        @Override
        public void writeBytes(byte[] buffer)
        {
            writeBytes(buffer, 0, buffer.length);
        }

        @Override
        public void writeBytes(byte[] bytes, int offset, int length)
        {
            while (length > 0) {
                int copied = min(buffer.length - bufferSize, length);
                arraycopy(bytes, offset, buffer, bufferSize, copied);
                bufferSize += copied;

                try {
                    flushBuffer(false);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                offset += copied;
                length -= copied;
            }
        }

        @Override
        public long getRetainedSize()
        {
            return sizeOf(buffer) + INSTANCE_SIZE;
        }

        @Override
        public void close()
                throws IOException
        {
            if (failed) {
                try {
                    abortUpload();
                    return;
                }
                catch (RuntimeException e) {
                    throw new IOException(e);
                }
            }

            try {
                flushBuffer(true);
                waitForPreviousUploadFinish();
            }
            catch (IOException | RuntimeException e) {
                abortUploadSuppressed(e);
                throw e;
            }

            try {
                uploadId.ifPresent(this::finishUpload);
            }
            catch (RuntimeException e) {
                abortUploadSuppressed(e);
                throw new IOException(e);
            }
        }

        private void flushBuffer(boolean finished)
                throws IOException
        {
            try {
                waitForPreviousUploadFinish();
            }
            catch (IOException e) {
                failed = true;
                abortUploadSuppressed(e);
                throw e;
            }

            // skip multipart upload if there would only be one part
            if (finished && uploadId.isEmpty()) {
                InputStream in = new ByteArrayInputStream(buffer, 0, bufferSize);

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(bufferSize);
                metadata.setContentMD5(getMd5AsBase64(buffer, 0, bufferSize));

                PutObjectRequest request = new PutObjectRequest(bucketName, key, in, metadata);
                sseCustomerKey.ifPresent(request::withSSECustomerKey);

                try {
                    s3.putObject(request);
                    return;
                }
                catch (AmazonServiceException e) {
                    throw new IOException(e);
                }
            }

            // The multipart upload API only accept the last part to be less than 5MB
            if (bufferSize == buffer.length || (finished && bufferSize > 0)) {
                byte[] data = buffer;
                int length = bufferSize;
                this.buffer = new byte[buffer.length];
                bufferSize = 0;

                inProgressUploadFuture = uploadExecutor.submit(() -> uploadPage(data, length));
            }
        }

        private UploadPartResult uploadPage(byte[] data, int length)
        {
            if (uploadId.isEmpty()) {
                uploadId = Optional.of(uploadIdFactory.get());
            }

            currentPartNumber++;
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withUploadId(uploadId.get())
                    .withPartNumber(currentPartNumber)
                    .withInputStream(new ByteArrayInputStream(data, 0, length))
                    .withPartSize(length)
                    .withMD5Digest(getMd5AsBase64(data, 0, length));
            sseCustomerKey.ifPresent(uploadRequest::withSSECustomerKey);

            UploadPartResult partResult = s3.uploadPart(uploadRequest);
            parts.add(partResult);
            return partResult;
        }

        private static String getMd5AsBase64(byte[] data, int offset, int length)
        {
            @SuppressWarnings("deprecation")
            byte[] md5 = md5().hashBytes(data, offset, length).asBytes();
            return Base64.getEncoder().encodeToString(md5);
        }

        private void waitForPreviousUploadFinish()
                throws IOException
        {
            if (inProgressUploadFuture == null) {
                return;
            }

            try {
                inProgressUploadFuture.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
            catch (ExecutionException e) {
                throw new IOException("Streaming upload failed", e);
            }
        }

        private void finishUpload(String uploadId)
        {
            List<PartETag> etags = parts.stream()
                    .map(UploadPartResult::getPartETag)
                    .collect(toList());
            s3.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, etags));
        }

        private void abortUpload()
        {
            uploadId.ifPresent(id -> s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, id)));
        }

        @SuppressWarnings("ObjectEquality")
        private void abortUploadSuppressed(Throwable throwable)
        {
            try {
                abortUpload();
            }
            catch (Throwable t) {
                if (throwable != t) {
                    throwable.addSuppressed(t);
                }
            }
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset(int position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writableBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWritable()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeByte(int value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeShort(int value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeLong(long value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeFloat(float v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDouble(double value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBytes(Slice source, int sourceIndex, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBytes(InputStream in, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice slice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getUnderlyingSlice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString(Charset charset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendLong(long value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendDouble(double value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendInt(int value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendShort(int value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendByte(int value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendBytes(byte[] source, int sourceIndex, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendBytes(byte[] source)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendBytes(Slice slice)
        {
            throw new UnsupportedOperationException();
        }
    }
}
