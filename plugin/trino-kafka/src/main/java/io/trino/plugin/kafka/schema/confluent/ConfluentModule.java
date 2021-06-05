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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroBytesDeserializer;
import io.trino.decoder.avro.AvroDeserializer;
import io.trino.decoder.avro.AvroReaderSupplier;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.plugin.kafka.SessionPropertiesProvider;
import io.trino.plugin.kafka.decoder.KafkaRowDecoderFactory;
import io.trino.plugin.kafka.encoder.DispatchingRowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.avro.AvroRowEncoder;
import io.trino.plugin.kafka.encoder.kafka.KafkaSerializerRowEncoder;
import io.trino.plugin.kafka.schema.ContentSchemaReader;
import io.trino.plugin.kafka.schema.ForKafkaRead;
import io.trino.plugin.kafka.schema.ForKafkaWrite;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.HostAddress;

import javax.inject.Singleton;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.kafka.encoder.EncoderModule.encoderFactory;
import static java.util.Objects.requireNonNull;

public class ConfluentModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
        install(new ConfluentDecoderModule());
        install(new ConfluentEncoderModule());
        binder.bind(AvroConfluentContentSchemaReader.class).in(SINGLETON);
        binder.bind(ContentSchemaReader.class).annotatedWith(ForKafkaRead.class).to(AvroConfluentContentSchemaReader.class);
        newSetBinder(binder, SchemaRegistryClientPropertiesProvider.class);
        newSetBinder(binder, SchemaProvider.class).addBinding().to(AvroSchemaProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(ConfluentSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TableDescriptionSupplier.class).toProvider(ConfluentSchemaRegistryTableDescriptionSupplier.Factory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, SchemaParser.class).addBinding("AVRO").to(AvroSchemaParser.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static SchemaRegistryClient createSchemaRegistryClient(
            ConfluentSchemaRegistryConfig confluentConfig,
            Set<SchemaProvider> schemaProviders,
            Set<SchemaRegistryClientPropertiesProvider> propertiesProviders,
            ClassLoader classLoader)
    {
        requireNonNull(confluentConfig, "confluentConfig is null");
        requireNonNull(schemaProviders, "schemaProviders is null");
        requireNonNull(propertiesProviders, "propertiesProviders is null");

        List<String> baseUrl = confluentConfig.getConfluentSchemaRegistryUrls().stream()
                .map(HostAddress::getHostText)
                .collect(toImmutableList());

        Map<String, ?> schemaRegistryClientProperties = propertiesProviders.stream()
                .map(SchemaRegistryClientPropertiesProvider::getSchemaRegistryClientProperties)
                .flatMap(properties -> properties.entrySet().stream())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return new ClassLoaderSafeSchemaRegistryClient(
                new CachedSchemaRegistryClient(
                        baseUrl,
                        confluentConfig.getConfluentSchemaRegistryClientCacheSize(),
                        ImmutableList.copyOf(schemaProviders),
                        schemaRegistryClientProperties),
                classLoader);
    }

    private static class ConfluentDecoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AvroReaderSupplier.Factory.class).to(ConfluentAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
            binder.bind(AvroDeserializer.Factory.class).to(AvroBytesDeserializer.Factory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(KafkaRowDecoderFactory.NAME).to(KafkaRowDecoderFactory.class).in(SINGLETON);
            binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
        }
    }

    private static class ConfluentEncoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(ConfluentRowEncoderFactory.class).in(SINGLETON);
            MapBinder<String, RowEncoderFactory> encoderFactoriesByName = encoderFactory(binder);
            encoderFactoriesByName.addBinding(AvroRowEncoder.NAME).to(ConfluentRowEncoderFactory.class).in(SINGLETON);
            encoderFactoriesByName.addBinding(KafkaRowDecoderFactory.NAME).to(KafkaSerializerRowEncoder.Factory.class).in(SINGLETON);
            binder.bind(DispatchingRowEncoderFactory.class).in(SINGLETON);
            binder.bind(ContentSchemaReader.class).annotatedWith(ForKafkaWrite.class).to(AvroConfluentContentSchemaReader.class).in(SINGLETON);
        }
    }
}
