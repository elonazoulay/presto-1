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
package io.prestosql.plugin.kafka.schema.confluent;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.prestosql.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy;

import javax.validation.constraints.NotNull;

import static io.prestosql.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;

public class ConfluentSchemaRegistryConfig
{
    private String confluentSchemaRegistryUrl;
    private int confluentSchemaRegistryClientCacheSize = 1000;
    private EmptyFieldStrategy emptyFieldStrategy = IGNORE;

    @NotNull
    public String getConfluentSchemaRegistryUrl()
    {
        return confluentSchemaRegistryUrl;
    }

    @Config("kafka.confluent-schema-registry-url")
    @ConfigDescription("The url of the Confluent Schema Registry")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryUrl(String confluentSchemaRegistryUrl)
    {
        this.confluentSchemaRegistryUrl = confluentSchemaRegistryUrl;
        return this;
    }

    public int getConfluentSchemaRegistryClientCacheSize()
    {
        return confluentSchemaRegistryClientCacheSize;
    }

    @Config("kafka.confluent-schema-registry-client-cache-size")
    @ConfigDescription("The maximum number of subjects that can be stored in the Confluent Schema Registry client cache")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryClientCacheSize(int confluentSchemaRegistryClientCacheSize)
    {
        this.confluentSchemaRegistryClientCacheSize = confluentSchemaRegistryClientCacheSize;
        return this;
    }

    public EmptyFieldStrategy getEmptyFieldStrategy()
    {
        return emptyFieldStrategy;
    }

    @Config("kafka.empty-field-strategy")
    @ConfigDescription("How to handle struct types with no fields: ignore, add a boolean field named 'dummy' or fail the query")
    public ConfluentSchemaRegistryConfig setEmptyFieldStrategy(EmptyFieldStrategy emptyFieldStrategy)
    {
        this.emptyFieldStrategy = emptyFieldStrategy;
        return this;
    }
}
