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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.decoder.KafkaRowDecoderFactory;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.KafkaErrorCode.SCHEMA_REGISTRY_AMBIGUOUS_SUBJECT;
import static io.trino.plugin.kafka.encoder.kafka.KafkaSerializerRowEncoder.fromTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.joining;

public class ConfluentSchemaRegistryTableDescriptionSupplier
        implements TableDescriptionSupplier
{
    public static final String NAME = "confluent";

    private static final String KEY_VALUE_PAIR_DELIMITER = "&";
    private static final String KEY_VALUE_DELIMITER = "=";

    private static final String KEY_SUBJECT = "key-subject";

    private static final String VALUE_SUBJECT = "value-subject";

    private static final String KEY_COLUMNS = "key-columns";
    private static final String KEY_COLUMN_TYPE_DELIMITER = ":";
    private static final String KEY_SUFFIX = "-key";
    private static final String VALUE_SUFFIX = "-value";

    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, SchemaParser> schemaParsers;
    private final String defaultSchema;
    private final Supplier<SetMultimap<String, TopicAndSubjects>> topicAndSubjectsSupplier;
    private final Supplier<SetMultimap<String, String>> subjectsSupplier;
    private final TypeManager typeManager;

    public ConfluentSchemaRegistryTableDescriptionSupplier(
            SchemaRegistryClient schemaRegistryClient,
            Map<String, SchemaParser> schemaParsers,
            String defaultSchema,
            Duration subjectsCacheRefreshInterval,
            TypeManager typeManager)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.schemaParsers = ImmutableMap.copyOf(requireNonNull(schemaParsers, "schemaParsers is null"));
        this.defaultSchema = requireNonNull(defaultSchema, "defaultSchema is null");
        topicAndSubjectsSupplier = memoizeWithExpiration(this::getTopicAndSubjects, subjectsCacheRefreshInterval.toMillis(), MILLISECONDS);
        subjectsSupplier = memoizeWithExpiration(this::getAllSubjects, subjectsCacheRefreshInterval.toMillis(), MILLISECONDS);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public static class Factory
            implements Provider<TableDescriptionSupplier>
    {
        private final SchemaRegistryClient schemaRegistryClient;
        private final Map<String, SchemaParser> schemaParsers;
        private final String defaultSchema;
        private final Duration subjectsCacheRefreshInterval;
        private final TypeManager typeManager;

        @Inject
        public Factory(
                SchemaRegistryClient schemaRegistryClient,
                Map<String, SchemaParser> schemaParsers,
                KafkaConfig kafkaConfig,
                ConfluentSchemaRegistryConfig confluentConfig,
                TypeManager typeManager)
        {
            this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
            this.schemaParsers = ImmutableMap.copyOf(requireNonNull(schemaParsers, "schemaParsers is null"));
            requireNonNull(kafkaConfig, "kafkaConfig is null");
            this.defaultSchema = kafkaConfig.getDefaultSchema();
            requireNonNull(confluentConfig, "confluentConfig is null");
            this.subjectsCacheRefreshInterval = confluentConfig.getConfluentSubjectsCacheRefreshInterval();
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public TableDescriptionSupplier get()
        {
            return new ConfluentSchemaRegistryTableDescriptionSupplier(schemaRegistryClient, schemaParsers, defaultSchema, subjectsCacheRefreshInterval, typeManager);
        }
    }

    private String resolveSubject(String candidate)
    {
        Collection<String> subjects = subjectsSupplier.get().get(candidate);
        checkState(!subjects.isEmpty(), "Subject '%s' not found", candidate);
        if (subjects.size() != 1) {
            throw new TrinoException(
                    SCHEMA_REGISTRY_AMBIGUOUS_SUBJECT,
                    format(
                            "Subject '%s' is ambiguous, and may refer to one of the following: %s",
                            candidate,
                            join(", ", subjects)));
        }
        return getOnlyElement(subjects);
    }

    private SetMultimap<String, String> getAllSubjects()
    {
        try {
            return schemaRegistryClient.getAllSubjects().stream()
                    .collect(toImmutableSetMultimap(subject -> subject.toLowerCase(ENGLISH), identity()));
        }
        catch (IOException | RestClientException e) {
            throw new RuntimeException("Failed to retrieve subjects from schema registry", e);
        }
    }

    // Refresh mapping of topic to subjects.
    // *Note: that this mapping only supports subjects that use the SubjectNameStrategy of TopicName.
    // If another SubjectNameStrategy is used then the key and value subject must be encoded
    // into the table name as follows:
    // <table name>[&key-subject=<key subject>][&value-subject=<value subject]
    // ex. kafka.default."my-topic&key-subject=foo&value-subject=bar"
    private SetMultimap<String, TopicAndSubjects> getTopicAndSubjects()
    {
        ImmutableSetMultimap.Builder<String, String> topicToSubjectsBuilder = ImmutableSetMultimap.builder();
        for (String subject : subjectsSupplier.get().values()) {
            if (isValidSubject(subject)) {
                topicToSubjectsBuilder.put(extractTopicFromSubject(subject), subject);
            }
        }
        ImmutableSetMultimap.Builder<String, TopicAndSubjects> topicSubjectsCacheBuilder = ImmutableSetMultimap.builder();
        for (Map.Entry<String, Collection<String>> entry : topicToSubjectsBuilder.build().asMap().entrySet()) {
            String topic = entry.getKey();
            TopicAndSubjects topicAndSubjects = new TopicAndSubjects(
                    topic,
                    getKeySubjectFromTopic(topic, entry.getValue()),
                    getValueSubjectFromTopic(topic, entry.getValue()));
            topicSubjectsCacheBuilder.put(topicAndSubjects.getTableName(), topicAndSubjects);
        }
        return topicSubjectsCacheBuilder.build();
    }

    @Override
    public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        TopicAndSubjects topicAndSubjects = parseTopicAndSubjects(schemaTableName);

        String tableName = topicAndSubjects.getTableName();
        if (topicAndSubjectsSupplier.get().containsKey(tableName)) {
            // Use the topic from cache, if present, in case the topic is mixed case
            Collection<TopicAndSubjects> topicAndSubjectsCollection = topicAndSubjectsSupplier.get().get(tableName);
            if (topicAndSubjectsCollection.size() != 1) {
                throw new TrinoException(
                        SCHEMA_REGISTRY_AMBIGUOUS_SUBJECT,
                        format(
                                "Unable to access '%s' table. Subject is ambiguous, and may refer to one of the following: %s",
                                schemaTableName.getTableName(),
                                topicAndSubjectsCollection.stream().map(TopicAndSubjects::getTopic).collect(joining(", "))));
            }
            TopicAndSubjects topicAndSubjectsFromCache = getOnlyElement(topicAndSubjectsCollection);
            topicAndSubjects = new TopicAndSubjects(
                    topicAndSubjectsFromCache.getTopic(),
                    topicAndSubjects.getKeySubject().or(topicAndSubjectsFromCache::getKeySubject),
                    topicAndSubjects.getValueSubject().or(topicAndSubjectsFromCache::getValueSubject),
                    topicAndSubjects.getKeyColumns());
        }

        if (topicAndSubjects.getKeySubject().isEmpty() && topicAndSubjects.getValueSubject().isEmpty()) {
            return Optional.empty();
        }
        Optional<KafkaTopicFieldGroup> key = topicAndSubjects.getKeySubject().map(subject -> getFieldGroup(session, subject));
        if (key.isEmpty() && topicAndSubjects.getKeyColumns().isPresent()) {
            key = Optional.of(getFieldGroupFromKeyColumns(topicAndSubjects.getKeyColumns().get()));
        }
        Optional<KafkaTopicFieldGroup> message = topicAndSubjects.getValueSubject().map(subject -> getFieldGroup(session, subject));
        return Optional.of(new KafkaTopicDescription(tableName, Optional.of(schemaTableName.getSchemaName()), topicAndSubjects.getTopic(), key, message, topicAndSubjects.getKeyColumns().map(this::getKeyColumnNames)));
    }

    private List<String> getKeyColumnNames(List<String> keyColumns)
    {
        Splitter splitter = Splitter.on(KEY_COLUMN_TYPE_DELIMITER).omitEmptyStrings().trimResults();
        return keyColumns.stream().map(nameAndType -> splitter.splitToList(nameAndType).get(0))
                .collect(toImmutableList());
    }

    private KafkaTopicFieldGroup getFieldGroupFromKeyColumns(List<String> keyColumns)
    {
        String keyColumnNameAndType = getOnlyElement(keyColumns);
        List<String> nameAndType = Splitter.on(KEY_COLUMN_TYPE_DELIMITER).omitEmptyStrings().trimResults().splitToList(keyColumnNameAndType);
        Type keyColumnType = typeManager.getType(TypeId.of(nameAndType.get(1)));
        return new KafkaTopicFieldGroup(KafkaRowDecoderFactory.NAME, Optional.of(fromTrinoType(keyColumnType).toString()), Optional.empty(), keyColumns.stream()
                .map(keyColumn -> getFieldDescriptionFromKeyColumn(keyColumn))
                .collect(toImmutableList()));
    }

    private KafkaTopicFieldDescription getFieldDescriptionFromKeyColumn(String keyColumn)
    {
        List<String> nameAndType = Splitter.on(KEY_COLUMN_TYPE_DELIMITER).omitEmptyStrings().trimResults().splitToList(keyColumn);
        checkState(nameAndType.size() == 2, "Unexpected format for key column/type. Should be <name>:<type>");
        return new KafkaTopicFieldDescription(
                nameAndType.get(0),
                typeManager.getType(TypeId.of(nameAndType.get(1))),
                nameAndType.get(0),
                null,
                null,
                null,
                false);
    }

    private KafkaTopicFieldGroup getFieldGroup(ConnectorSession session, String subject)
    {
        SchemaMetadata schemaMetadata = getLatestSchemaMetadata(subject);
        SchemaParser schemaParser = schemaParsers.get(schemaMetadata.getSchemaType());
        if (schemaParser == null) {
            throw new TrinoException(NOT_SUPPORTED, "Not supported schema: " + schemaMetadata.getSchemaType());
        }
        return schemaParser.parse(
                session,
                subject,
                schemaRegistryClient.parseSchema(
                        schemaMetadata.getSchemaType(),
                        schemaMetadata.getSchema(),
                        schemaMetadata.getReferences())
                        .orElseThrow());
    }

    private SchemaMetadata getLatestSchemaMetadata(String subject)
    {
        try {
            return schemaRegistryClient.getLatestSchemaMetadata(subject);
        }
        catch (IOException | RestClientException e) {
            throw new RuntimeException(format("Unable to get field group for '%s' subject", subject), e);
        }
    }

    // If the default subject naming strategy is not used, the key and value subjects
    // can be specified by adding them to the table name as follows:
    //  <tablename>&key-subject=<key subject>&value-subject=<value subject>
    // ex. kafka.default."mytable&key-subject=foo&value-subject=bar"
    private TopicAndSubjects parseTopicAndSubjects(SchemaTableName encodedSchemaTableName)
    {
        String encodedTableName = encodedSchemaTableName.getTableName();
        List<String> parts = Splitter.on(KEY_VALUE_PAIR_DELIMITER).trimResults().splitToList(encodedTableName);
        checkState(!parts.isEmpty() && parts.size() <= 4, "Unexpected format for encodedTableName. Expected format is <tableName>[&key-subject=<key subject>][&value-subject=<value subject>][key-columns=<key_column,key_column,...>]");
        String tableName = parts.get(0);
        Optional<String> keySubject = Optional.empty();
        Optional<String> valueSubject = Optional.empty();
        Optional<List<String>> keyColumns = Optional.empty();
        for (int part = 1; part < parts.size(); part++) {
            List<String> subjectKeyValue = Splitter.on(KEY_VALUE_DELIMITER).trimResults().splitToList(parts.get(part));
            checkState(subjectKeyValue.size() == 2
                            && (subjectKeyValue.get(0).equals(KEY_SUBJECT)
                            || subjectKeyValue.get(0).equals(VALUE_SUBJECT)
                            || subjectKeyValue.get(0).equals(KEY_COLUMNS)),
                    "Unexpected parameter '%s', should be [%s=<key subject>' | %s=<value subject> | %s=<key_column,key_column...> ]", parts.get(part),
                    KEY_SUBJECT,
                    VALUE_SUBJECT,
                    KEY_COLUMNS);
            if (subjectKeyValue.get(0).equals(KEY_SUBJECT)) {
                checkState(keySubject.isEmpty(), "Key subject already defined");
                keySubject = Optional.of(subjectKeyValue.get(1))
                        .map(this::resolveSubject);
            }
            else if (subjectKeyValue.get(0).equals(VALUE_SUBJECT)) {
                checkState(valueSubject.isEmpty(), "Value subject already defined");
                valueSubject = Optional.of(subjectKeyValue.get(1))
                        .map(this::resolveSubject);
            }
            else if (subjectKeyValue.get(0).equals(KEY_COLUMNS)) {
                checkState(keyColumns.isEmpty(), "keyColumns already defined");
                keyColumns = Optional.of(Splitter.on(',').omitEmptyStrings().trimResults().splitToList(subjectKeyValue.get(1)));
                checkState(!keyColumns.get().isEmpty(), "keyColumns is empty");
            }
            else {
                throw new IllegalStateException(format("Invalid key '%s'", subjectKeyValue.get(0)));
            }
        }
        return new TopicAndSubjects(tableName, keySubject, valueSubject, keyColumns);
    }

    @Override
    public Set<SchemaTableName> listTables()
    {
        return topicAndSubjectsSupplier.get().keySet().stream()
                .map(tableName -> new SchemaTableName(defaultSchema, tableName))
                .collect(toImmutableSet());
    }

    private static boolean isValidSubject(String subject)
    {
        requireNonNull(subject, "subject is null");
        return (subject.endsWith(VALUE_SUFFIX) && subject.length() > VALUE_SUFFIX.length())
                || (subject.endsWith(KEY_SUFFIX) && subject.length() > KEY_SUFFIX.length());
    }

    private static String extractTopicFromSubject(String subject)
    {
        requireNonNull(subject, "subject is null");
        String topic;
        if (subject.endsWith(VALUE_SUFFIX)) {
            topic = subject.substring(0, subject.length() - VALUE_SUFFIX.length());
        }
        else {
            checkState(subject.endsWith(KEY_SUFFIX), "Unexpected subject name %s", subject);
            topic = subject.substring(0, subject.length() - KEY_SUFFIX.length());
        }
        checkArgument(!topic.isEmpty(), "Unexpected subject name %s", subject);
        return topic;
    }

    private static Optional<String> getKeySubjectFromTopic(String topic, Collection<String> subjectsForTopic)
    {
        String keySubject = topic + KEY_SUFFIX;
        if (subjectsForTopic.contains(keySubject)) {
            return Optional.of(keySubject);
        }
        return Optional.empty();
    }

    private static Optional<String> getValueSubjectFromTopic(String topic, Collection<String> subjectsForTopic)
    {
        String valueSubject = topic + VALUE_SUFFIX;
        if (subjectsForTopic.contains(valueSubject)) {
            return Optional.of(valueSubject);
        }
        return Optional.empty();
    }

    private static class TopicAndSubjects
    {
        private final Optional<String> keySubject;
        private final Optional<String> valueSubject;
        private final Optional<List<String>> keyColumns;
        private final String topic;

        public TopicAndSubjects(String topic, Optional<String> keySubject, Optional<String> valueSubject)
        {
            this(topic, keySubject, valueSubject, Optional.empty());
        }

        public TopicAndSubjects(String topic, Optional<String> keySubject, Optional<String> valueSubject, Optional<List<String>> keyColumns)
        {
            this.topic = requireNonNull(topic, "topic is null");
            this.keySubject = requireNonNull(keySubject, "keySubject is null");
            this.valueSubject = requireNonNull(valueSubject, "valueSubject is null");
            this.keyColumns = requireNonNull(keyColumns, "keyColumns is null");
        }

        public String getTableName()
        {
            return topic.toLowerCase(ENGLISH);
        }

        public String getTopic()
        {
            return topic;
        }

        public Optional<String> getKeySubject()
        {
            return keySubject;
        }

        public Optional<String> getValueSubject()
        {
            return valueSubject;
        }

        public Optional<List<String>> getKeyColumns()
        {
            return keyColumns;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TopicAndSubjects)) {
                return false;
            }
            TopicAndSubjects that = (TopicAndSubjects) other;
            return topic.equals(that.topic) &&
                    keySubject.equals(that.keySubject) &&
                    valueSubject.equals(that.valueSubject) &&
                    keyColumns.equals(that.keyColumns);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(topic, keySubject, valueSubject, keyColumns);
        }
    }
}
