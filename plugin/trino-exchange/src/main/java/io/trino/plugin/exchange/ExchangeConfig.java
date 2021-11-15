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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class ExchangeConfig
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private String s3Bucket;

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("exchange.s3.aws-access-key")
    public ExchangeConfig setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("exchange.s3.aws-secret-key")
    @ConfigSecuritySensitive
    public ExchangeConfig setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public String getS3Bucket()
    {
        return s3Bucket;
    }

    @Config("exchange.s3.bucket")
    public ExchangeConfig setS3Bucket(String s3Bucket)
    {
        this.s3Bucket = s3Bucket;
        return this;
    }
}
