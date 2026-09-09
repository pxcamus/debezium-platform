/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.platform.environment.operator;

import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.DATABASE;
import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.DEBEZIUM_DATABASE_NAME_CONFIG;
import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.DEBEZIUM_DATABASE_USERNAME_CONFIG;
import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.DEBEZIUM_SQLSERVER_DATABASE_NAME_CONFIG;
import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.USERNAME;
import static io.debezium.platform.environment.database.DatabaseConnectionFactory.DATABASE_CONNECTION_CONFIGURATION_PREFIX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;

import io.debezium.operator.api.model.ConfigProperties;
import io.debezium.operator.api.model.DebeziumServer;
import io.debezium.operator.api.model.DebeziumServerBuilder;
import io.debezium.operator.api.model.DebeziumServerSpecBuilder;
import io.debezium.operator.api.model.Predicate;
import io.debezium.operator.api.model.PredicateBuilder;
import io.debezium.operator.api.model.Quarkus;
import io.debezium.operator.api.model.QuarkusBuilder;
import io.debezium.operator.api.model.Sink;
import io.debezium.operator.api.model.SinkBuilder;
import io.debezium.operator.api.model.Transformation;
import io.debezium.operator.api.model.TransformationBuilder;
import io.debezium.operator.api.model.runtime.Runtime;
import io.debezium.operator.api.model.runtime.RuntimeApiBuilder;
import io.debezium.operator.api.model.runtime.RuntimeBuilder;
import io.debezium.operator.api.model.runtime.RuntimeEnvironment;
import io.debezium.operator.api.model.runtime.metrics.Metrics;
import io.debezium.operator.api.model.runtime.storage.RuntimeStorage;
import io.debezium.operator.api.model.runtime.templates.ContainerEnvVar;
import io.debezium.operator.api.model.runtime.templates.ContainerTemplate;
import io.debezium.operator.api.model.runtime.templates.Probe;
import io.debezium.operator.api.model.runtime.templates.Probes;
import io.debezium.operator.api.model.runtime.templates.Templates;
import io.debezium.operator.api.model.source.Offset;
import io.debezium.operator.api.model.source.OffsetBuilder;
import io.debezium.operator.api.model.source.SchemaHistory;
import io.debezium.operator.api.model.source.SchemaHistoryBuilder;
import io.debezium.operator.api.model.source.Source;
import io.debezium.operator.api.model.source.SourceBuilder;
import io.debezium.operator.api.model.source.storage.CustomStoreBuilder;
import io.debezium.platform.config.PipelineConfigGroup;
import io.debezium.platform.data.model.ConnectionEntity;
import io.debezium.platform.domain.VaultService;
import io.debezium.platform.domain.views.PipelineComponent;
import io.debezium.platform.domain.views.Transform;
import io.debezium.platform.domain.views.Vault;
import io.debezium.platform.domain.views.flat.PipelineFlat;
import io.debezium.platform.domain.views.refs.VaultReference;
import io.debezium.platform.environment.operator.configuration.TableNameResolver;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountTokenProjectionBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeProjectionBuilder;

@ApplicationScoped
public class PipelineMapper {

    private static final String SIGNAL_ENABLED_CHANNELS_CONFIG = "signal.enabled.channels";
    private static final String NOTIFICATION_ENABLED_CHANNELS_CONFIG = "notification.enabled.channels";
    private static final String DEFAULT_SIGNAL_CHANNELS = "source,in-process";
    private static final String DEFAULT_NOTIFICATION_CHANNELS = "log";
    private static final String PREDICATE_PREFIX = "p";
    private static final String PREDICATE_ALIAS_FORMAT = "%s%s";
    private static final String QUARKUS_LOG_CATEGORY_FORMAT = "log.category.\"%s\".level";
    private static final String MIN_LOG_LEVEL = "TRACE";
    private static final String LOG_MIN_LEVEL_PROP_NAME = "log.min-level";
    private static final String LOG_LEVEL_PROP_NAME = "log.level";
    private static final String LOG_CONSOLE_JSON_PROP_NAME = "log.console.json";
    private static final List<String> RESOLVABLE_CONFIGS = List.of("jdbc.schema.history.table.name", "jdbc.offset.table.name");

    private static final String SERVICE_ACCOUNT_NAME_FORMAT = "%s-sa";
    private static final String VAULT_TOKEN_FILE_NAME = "token";
    private static final String VAULT_TOKEN_MOUNT_FORMAT = "/debezium/external/%s/%s";
    private static final String VAULT_ENV_PREFIX = "DEBEZIUM_VAULT_";
    private static final String VAULT_NAMES_ENV = VAULT_ENV_PREFIX + "NAMES";
    private static final String VAULT_PATH_ITEM = "path";

    private static final String KAFKA_CONNECTION_CONFIGURATION_PREFIX = "producer.";
    private static final String MONGODB_CONNECTION_CONFIGURATION_PREFIX = "mongodb.";
    private static final String PULSAR_CONNECTION_CONFIGURATION_PREFIX = "pulsar.client.";
    private static final String RABBITMQ_CONNECTION_CONFIGURATION_PREFIX = "rabbitmq.connection.";
    private static final String RABBITMQ_STREAM_CONNECTION_CONFIGURATION_PREFIX = "rabbitmqstream.connection.";
    private static final String JDBC_CONNECTION_CONFIGURATION_PREFIX = "connection.";

    private static final String SERVER_SINK_FQCN_PREFIX = "io.debezium.server.";
    private static final Map<String, String> SINK_TYPE_OVERRIDES = Map.of(
            "io.debezium.server.pubsub.PubSubLiteChangeConsumer", "pubsublite",
            "io.debezium.server.rabbitmq.RabbitMqStreamNativeChangeConsumer", "rabbitmqstream",
            "io.debezium.server.nats.jetstream.NatsJetStreamChangeConsumer", "nats-jetstream",
            "io.debezium.server.nats.streaming.NatsStreamingChangeConsumer", "nats-streaming");

    final PipelineConfigGroup pipelineConfigGroup;
    final TableNameResolver tableNameResolver;
    final Metrics metrics;
    final VaultService vaultService;

    public PipelineMapper(PipelineConfigGroup pipelineConfigGroup,
                          TableNameResolver tableNameResolver,
                          Metrics metrics,
                          VaultService vaultService) {
        this.pipelineConfigGroup = pipelineConfigGroup;
        this.tableNameResolver = tableNameResolver;
        this.metrics = metrics;
        this.vaultService = vaultService;
    }

    public DebeziumServer map(PipelineFlat pipeline) {

        var dsQuarkus = createQuarkus(pipeline);

        var dsRuntime = createRuntime(pipeline);

        var dsSource = createSource(pipeline);

        var dsSink = createSink(pipeline);

        List<Transformation> transformations = pipeline.getTransforms().stream()
                .map(this::buildTransformation)
                .toList();

        Map<String, Predicate> predicates = pipeline.getTransforms().stream()
                .filter(this::hasPredicate)
                .collect(Collectors.toMap(
                        this::getPredicateName,
                        this::buildPredicate));

        var specBuilder = new DebeziumServerSpecBuilder()
                .withQuarkus(dsQuarkus)
                .withRuntime(dsRuntime)
                .withSource(dsSource)
                .withSink(dsSink)
                .withTransforms(transformations)
                .withPredicates(predicates);

        // Only set image if configured, otherwise let operator's ServerImageProvider determine it
        pipelineConfigGroup.server().image().ifPresent(specBuilder::withImage);

        return new DebeziumServerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(pipeline.getName())
                        .withLabels(createLabels(pipeline))
                        .build())
                .withSpec(specBuilder.build())
                .build();
    }

    private Map<String, String> createLabels(PipelineFlat pipeline) {
        Map<String, String> labels = new HashMap<>(pipelineConfigGroup.labels());
        labels.put(OperatorPipelineController.LABEL_DBZ_CONDUCTOR_ID, pipeline.getId().toString());
        return labels;
    }

    private Quarkus createQuarkus(PipelineFlat pipeline) {
        var quarkusConfig = new ConfigProperties();

        Map<String, Object> basicLogProperties = Map.of(
                LOG_LEVEL_PROP_NAME, pipeline.getDefaultLogLevel(),
                LOG_MIN_LEVEL_PROP_NAME, MIN_LOG_LEVEL,
                LOG_CONSOLE_JSON_PROP_NAME, false);

        quarkusConfig.setAllProps(basicLogProperties);
        quarkusConfig.setAllProps(extractCategoriesLogs(pipeline));

        return new QuarkusBuilder()
                .withConfig(quarkusConfig)
                .build();
    }

    private Runtime createRuntime(PipelineFlat pipeline) {
        var healthConfig = pipelineConfigGroup.health();

        var livenessProbe = new Probe();
        livenessProbe.setInitialDelaySeconds(healthConfig.liveness().initialDelaySeconds());
        livenessProbe.setPeriodSeconds(healthConfig.liveness().periodSeconds());
        livenessProbe.setTimeoutSeconds(healthConfig.liveness().timeoutSeconds());
        livenessProbe.setFailureThreshold(healthConfig.liveness().failureThreshold());

        var readinessProbe = new Probe();
        readinessProbe.setInitialDelaySeconds(healthConfig.readiness().initialDelaySeconds());
        readinessProbe.setPeriodSeconds(healthConfig.readiness().periodSeconds());
        readinessProbe.setTimeoutSeconds(healthConfig.readiness().timeoutSeconds());
        readinessProbe.setFailureThreshold(healthConfig.readiness().failureThreshold());

        var probes = new Probes();
        probes.setLiveness(livenessProbe);
        probes.setReadiness(readinessProbe);

        var container = new ContainerTemplate();
        container.setProbes(probes);

        var templates = new Templates();
        templates.setContainer(container);

        var runtimeBuilder = new RuntimeBuilder()
                .withApi(new RuntimeApiBuilder().withEnabled().build())
                .withMetrics(metrics)
                .withTemplates(templates);

        if (pipelineConfigGroup.vault().enabled()) {
            // The pod runs as its own account so the backend can tell pipelines apart, and so the
            // account can carry cloud workload-identity annotations where those are what bind it.
            // Naming it here also stops the operator creating one of its own: ServiceAccountDependent
            // declines when spec.runtime.serviceAccount is set, which is what makes the account ours
            // to create and to delete.
            runtimeBuilder.withServiceAccount(serviceAccountNameFor(pipeline));
            runtimeBuilder.withStorage(createVaultTokenStorage());
            createVaultEnvironment(pipeline).ifPresent(runtimeBuilder::withEnvironment);
        }

        return runtimeBuilder.build();
    }

    /**
     * Name of the ServiceAccount a pipeline pod runs as.
     * <p>
     * Matches the name the operator would have generated, so enabling workload identity takes over
     * the existing account rather than leaving an orphan beside it.
     * </p>
     */
    private static String serviceAccountNameFor(PipelineFlat pipeline) {
        return SERVICE_ACCOUNT_NAME_FORMAT.formatted(pipeline.getName());
    }

    /**
     * Builds the runtime storage carrying the projected backend token.
     * <p>
     * {@code runtime.storage.external} is the only place a {@code DebeziumServer} can express a
     * volume — neither {@code runtime.templates.pod} nor any other field accepts one — and the
     * operator mounts what it finds there at {@code /debezium/external/<name>}.
     * </p>
     */
    private RuntimeStorage createVaultTokenStorage() {
        var storage = new RuntimeStorage();
        storage.setExternal(List.of(createVaultTokenVolume()));
        return storage;
    }

    private Volume createVaultTokenVolume() {
        var vault = pipelineConfigGroup.vault();

        // The audience is the whole point: a token stamped for the secret backend cannot be replayed
        // against the Kubernetes API server. Leave it out and it silently defaults back to the API
        // server, undoing that.
        var tokenProjection = new ServiceAccountTokenProjectionBuilder()
                .withPath(VAULT_TOKEN_FILE_NAME)
                .withAudience(vault.audience())
                .withExpirationSeconds(vault.tokenExpirationSeconds())
                .build();

        return new VolumeBuilder()
                .withName(vault.volumeName())
                .withNewProjected()
                .withSources(new VolumeProjectionBuilder().withServiceAccountToken(tokenProjection).build())
                .endProjected()
                .build();
    }

    /**
     * Tells the pipeline where its secret stores are, as environment variables.
     * <p>
     * One set of coordinates per vault bound to this pipeline's source, destination or transforms.
     * The address and auth role are the platform's; the name and the path are the vault's, taken
     * from its {@code items}. A pipeline bound to nothing gets an identity and no coordinates —
     * the deliberate intermediate state where credentials are still written into its configuration.
     * </p>
     * <p>
     * Environment variables rather than configuration properties because {@code spec.quarkus.config}
     * renders under the {@code quarkus.} prefix and nothing in the custom resource renders a bare
     * {@code debezium.vault.*} property. MicroProfile maps {@code DEBEZIUM_VAULT_...} back onto the
     * dotted name when the server looks it up, so the destination is the same; only the route
     * differs.
     * </p>
     */
    private Optional<RuntimeEnvironment> createVaultEnvironment(PipelineFlat pipeline) {
        var vault = pipelineConfigGroup.vault();

        if (vault.address().isEmpty()) {
            return Optional.empty();
        }

        var bound = boundVaults(pipeline);
        if (bound.isEmpty()) {
            return Optional.empty();
        }

        String tokenPath = VAULT_TOKEN_MOUNT_FORMAT.formatted(vault.volumeName(), VAULT_TOKEN_FILE_NAME);

        List<ContainerEnvVar> vars = new ArrayList<>();
        // Naming the vaults explicitly, because the server cannot enumerate them from environment
        // variables — the vault name and the property beneath it are no longer distinguishable once
        // both are upper-cased.
        vars.add(envVar(VAULT_NAMES_ENV, String.join(",", bound.keySet())));
        bound.forEach((name, path) -> {
            String prefix = VAULT_ENV_PREFIX + name.toUpperCase(Locale.ROOT) + "_";
            vars.add(envVar(prefix + "ADDRESS", vault.address().get()));
            vars.add(envVar(prefix + "PATH", path));
            vars.add(envVar(prefix + "AUTH_ROLE", vault.authRole()));
            vars.add(envVar(prefix + "AUTH_TOKEN_PATH", tokenPath));
        });

        var environment = new RuntimeEnvironment();
        environment.setVars(vars);
        return Optional.of(environment);
    }

    /**
     * The vaults this pipeline is bound to, by name, each with the path it serves.
     * <p>
     * Walks this pipeline's components only, so a pod is told about the stores its own
     * configuration refers to and nothing else. The binding carries an id; the row is read here,
     * at deploy time, so a vault's path can be corrected without touching every component bound
     * to it, and so the pipeline view that arrives through the outbox need carry nothing more
     * than the reference it already does. Sorted by name for a stable resource: the bindings are
     * sets, and a reordering is a diff the operator would act on.
     * </p>
     * <p>
     * A vault without a {@code path} item is skipped rather than emitted half-formed; the
     * {@code ${vault::name/key}} reference that needs it then fails in the pod, naming the vault.
     * </p>
     */
    private Map<String, String> boundVaults(PipelineFlat pipeline) {
        Map<String, String> bound = new TreeMap<>();

        Stream.concat(Stream.of(pipeline.getSource(), pipeline.getDestination()), streamOf(pipeline.getTransforms()))
                .filter(Objects::nonNull)
                .map(PipelineComponent::getVaults)
                .filter(Objects::nonNull)
                .flatMap(Set::stream)
                .map(VaultReference::getId)
                .filter(Objects::nonNull)
                .map(vaultService::findById)
                .flatMap(Optional::stream)
                .forEach(vault -> pathOf(vault).ifPresent(path -> bound.putIfAbsent(vault.getName(), path)));

        return bound;
    }

    private static Optional<String> pathOf(Vault vault) {
        return Optional.ofNullable(vault.getItems())
                .map(items -> items.get(VAULT_PATH_ITEM))
                .filter(path -> !path.isBlank());
    }

    private static <T> Stream<T> streamOf(List<T> list) {
        return list == null ? Stream.empty() : list.stream();
    }

    private static ContainerEnvVar envVar(String name, String value) {
        var variable = new ContainerEnvVar();
        variable.setName(name);
        variable.setValue(value);
        return variable;
    }

    private Sink createSink(PipelineFlat pipeline) {

        var sink = pipeline.getDestination();
        var sinkConfig = new ConfigProperties();

        if (sink.getConnection() != null) { // backward compatibility
            String configPrefix = prefixResolver(sink.getConnection().getType());
            sink.getConnection().getConfig().forEach((configName, configValue) -> sinkConfig.setProps(configPrefix + configName, configValue));
        }

        sinkConfig.setAllProps(sink.getConfig());

        return new SinkBuilder()
                .withType(resolveSinkType(sink.getType()))
                .withConfig(sinkConfig)
                .build();
    }

    private Source createSource(PipelineFlat pipeline) {

        var source = pipeline.getSource();
        var sourceConfig = new ConfigProperties();

        if (source.getConnection() != null) { // backward compatibility
            ConnectionEntity.Type connectionType = source.getConnection().getType();
            String configPrefix = prefixResolver(connectionType);
            source.getConnection().getConfig()
                    .forEach((configName, configValue) -> sourceConfig.setProps(getName(connectionType, configName, configPrefix), configValue));
        }

        sourceConfig.setAllProps(source.getConfig());
        sourceConfig.setProps(SIGNAL_ENABLED_CHANNELS_CONFIG, DEFAULT_SIGNAL_CHANNELS);
        sourceConfig.setProps(NOTIFICATION_ENABLED_CHANNELS_CONFIG, DEFAULT_NOTIFICATION_CHANNELS);

        return new SourceBuilder()
                .withSourceClass(source.getType())
                .withOffset(getOffset(pipeline))
                .withSchemaHistory(getSchemaHistory(pipeline))
                .withConfig(sourceConfig)
                .build();
    }

    private static String getName(ConnectionEntity.Type connectionType, String configName, String configPrefix) {
        return switch (configName) {
            case USERNAME -> configPrefix + DEBEZIUM_DATABASE_USERNAME_CONFIG;
            case DATABASE -> configPrefix + databaseNameConfig(connectionType);
            default -> configPrefix + configName;
        };
    }

    private static String databaseNameConfig(ConnectionEntity.Type connectionType) {
        if (connectionType == ConnectionEntity.Type.SQLSERVER) {
            return DEBEZIUM_SQLSERVER_DATABASE_NAME_CONFIG;
        }
        return DEBEZIUM_DATABASE_NAME_CONFIG;
    }

    /**
     * Resolves the Debezium Server sink type from a fully qualified class name to the short
     * {@code @Named} identifier expected by {@code debezium.sink.type}.
     *
     * <p>The general rule extracts the first package segment after {@code io.debezium.server.}:
     * <ul>
     *   <li>{@code io.debezium.server.kafka.KafkaChangeConsumer} &rarr; {@code kafka}</li>
     *   <li>{@code io.debezium.server.kinesis.KinesisChangeConsumer} &rarr; {@code kinesis}</li>
     *   <li>{@code io.debezium.server.fluss.FlussChangeConsumer} &rarr; {@code fluss}</li>
     * </ul>
     *
     * <p>A few consumers use multi-level packages or share a package with another consumer,
     * making the heuristic insufficient. These are handled via {@link #SINK_TYPE_OVERRIDES}:
     * <ul>
     *   <li>{@code io.debezium.server.nats.jetstream.NatsJetStreamChangeConsumer} &rarr; {@code nats-jetstream}</li>
     *   <li>{@code io.debezium.server.pubsub.PubSubLiteChangeConsumer} &rarr; {@code pubsublite}</li>
     *   <li>{@code io.debezium.server.rabbitmq.RabbitMqStreamNativeChangeConsumer} &rarr; {@code rabbitmqstream}</li>
     * </ul>
     *
     * <p>If the type is already a short name (no dots), it is returned as-is for backward compatibility.
     *
     * @param type the sink type, either a FQCN or an already-resolved short name
     * @return the short sink type identifier
     */
    static String resolveSinkType(String type) {
        if (type == null || !type.contains(".")) {
            return type;
        }

        String override = SINK_TYPE_OVERRIDES.get(type);
        if (override != null) {
            return override;
        }

        if (type.startsWith(SERVER_SINK_FQCN_PREFIX)) {
            String afterPrefix = type.substring(SERVER_SINK_FQCN_PREFIX.length());
            int dotIndex = afterPrefix.indexOf('.');
            if (dotIndex > 0) {
                return afterPrefix.substring(0, dotIndex);
            }
        }

        return type;
    }

    private String prefixResolver(ConnectionEntity.Type connectionType) {
        return switch (connectionType) {
            case ORACLE, MYSQL, MARIADB, SQLSERVER, POSTGRESQL -> DATABASE_CONNECTION_CONFIGURATION_PREFIX;
            case MONGODB -> MONGODB_CONNECTION_CONFIGURATION_PREFIX;
            case KAFKA -> KAFKA_CONNECTION_CONFIGURATION_PREFIX;
            case AMAZON_KINESIS, APACHE_ROCKETMQ, QDRANT, MILVUS, INFINISPAN, PRAVEGA, NATS_JETSTREAM, NATS_STREAMING, REDIS, AZURE_EVENTS_HUBS, HTTP, GOOGLE_PUB_SUB,
                    AMAZON_SQS ->
                "";
            case APACHE_PULSAR -> PULSAR_CONNECTION_CONFIGURATION_PREFIX;
            case JDBC -> JDBC_CONNECTION_CONFIGURATION_PREFIX;
            case RABBITMQ_STREAM -> RABBITMQ_CONNECTION_CONFIGURATION_PREFIX;
            case RABBITMQ_NATIVE_STREAM -> RABBITMQ_STREAM_CONNECTION_CONFIGURATION_PREFIX;
        };
    }

    private Map<String, Object> extractCategoriesLogs(PipelineFlat pipeline) {
        return pipeline.getLogLevels().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> toQuarkusFormat(entry.getKey()),
                        Map.Entry::getValue,
                        (v1, v2) -> v1,
                        HashMap::new));
    }

    private Predicate buildPredicate(Transform transform) {

        var predicateConfig = new ConfigProperties();
        predicateConfig.setAllProps(transform.getPredicate().getConfig());

        return new PredicateBuilder()
                .withType(transform.getPredicate().getType())
                .withConfig(predicateConfig)
                .build();
    }

    private Transformation buildTransformation(Transform transform) {

        var transformConfig = new ConfigProperties();
        transformConfig.setAllProps(transform.getConfig());

        var builder = new TransformationBuilder()
                .withType(transform.getType())
                .withConfig(transformConfig);

        if (hasPredicate(transform)) {
            builder.withPredicate(getPredicateName(transform))
                    .withNegate(transform.getPredicate().isNegate());
        }

        return builder.build();
    }

    private boolean hasPredicate(Transform transform) {
        return transform.getPredicate() != null && transform.getPredicate().getType() != null;
    }

    private String getPredicateName(Transform transform) {
        return String.format(PREDICATE_ALIAS_FORMAT, PREDICATE_PREFIX, transform.getId());
    }

    private SchemaHistory getSchemaHistory(PipelineFlat pipeline) {

        var pipelineSchemaHistoryConfigs = pipelineConfigGroup.schema().config();
        var schemaHistoryType = pipelineConfigGroup.schema().internal();

        Map<String, String> schemaHistoryStorageConfigs = new HashMap<>(pipelineSchemaHistoryConfigs);
        ConfigProperties schemaHistoryProps = new ConfigProperties();
        schemaHistoryStorageConfigs.forEach(schemaHistoryProps::setProps);

        RESOLVABLE_CONFIGS.forEach(
                prop -> schemaHistoryProps.setProps(prop, tableNameResolver.resolve(pipeline, schemaHistoryStorageConfigs.get(prop))));

        return new SchemaHistoryBuilder().withStore(new CustomStoreBuilder()
                .withType(schemaHistoryType)
                .withConfig(schemaHistoryProps)
                .build()).build();
    }

    private Offset getOffset(PipelineFlat pipeline) {

        var pipelineOffsetConfigs = pipelineConfigGroup.offset().storage().config();
        var offsetType = pipelineConfigGroup.offset().storage().type();

        Map<String, String> offsetStorageConfigs = new HashMap<>(pipelineOffsetConfigs);
        ConfigProperties offsetStorageProps = new ConfigProperties();
        offsetStorageConfigs.forEach(offsetStorageProps::setProps);

        RESOLVABLE_CONFIGS.forEach(
                prop -> offsetStorageProps.setProps(prop, tableNameResolver.resolve(pipeline, pipelineOffsetConfigs.get(prop))));

        return new OffsetBuilder().withStore(new CustomStoreBuilder()
                .withType(offsetType)
                .withConfig(offsetStorageProps)
                .build()).build();
    }

    private static String toQuarkusFormat(String key) {
        return String.format(PipelineMapper.QUARKUS_LOG_CATEGORY_FORMAT, key);
    }
}
