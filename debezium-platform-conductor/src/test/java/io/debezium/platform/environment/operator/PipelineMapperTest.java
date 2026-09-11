/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.platform.environment.operator;

import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.DATABASE;
import static io.debezium.platform.environment.database.DatabaseConnectionConfiguration.USERNAME;
import static io.debezium.platform.environment.operator.OperatorPipelineController.LABEL_DBZ_CONDUCTOR_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.debezium.doc.FixFor;
import io.debezium.operator.api.model.runtime.metrics.Metrics;
import io.debezium.operator.api.model.runtime.metrics.MetricsBuilder;
import io.debezium.platform.config.PipelineConfigGroup;
import io.debezium.platform.data.model.ConnectionEntity;
import io.debezium.platform.domain.VaultService;
import io.debezium.platform.domain.views.Connection;
import io.debezium.platform.domain.views.Transform;
import io.debezium.platform.domain.views.Vault;
import io.debezium.platform.domain.views.flat.DestinationFlat;
import io.debezium.platform.domain.views.flat.PipelineFlat;
import io.debezium.platform.domain.views.flat.SourceFlat;
import io.debezium.platform.domain.views.refs.VaultReference;
import io.debezium.platform.environment.operator.configuration.TableNameResolver;
import io.debezium.platform.environment.operator.metrics.OpenTelemetryExporterStrategy;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PipelineMapperTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    PipelineConfigGroup pipelineConfigGroup;

    @Mock
    TableNameResolver tableNameResolver;

    @Mock
    VaultService vaultService;

    private PipelineMapper pipelineMapper;

    @BeforeEach
    void setUp() {

        when(tableNameResolver.resolve(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
        when(pipelineConfigGroup.labels()).thenReturn(Map.of());
        when(pipelineConfigGroup.monitoring().otel().enabled()).thenReturn(false);
        when(pipelineConfigGroup.monitoring().otel().jmxIntervalMs()).thenReturn(1000);
        when(pipelineConfigGroup.monitoring().otel().metricExportIntervalMs()).thenReturn(5000);

        when(pipelineConfigGroup.health().liveness().initialDelaySeconds()).thenReturn(30);
        when(pipelineConfigGroup.health().liveness().periodSeconds()).thenReturn(10);
        when(pipelineConfigGroup.health().liveness().timeoutSeconds()).thenReturn(10);
        when(pipelineConfigGroup.health().liveness().failureThreshold()).thenReturn(3);
        when(pipelineConfigGroup.health().readiness().initialDelaySeconds()).thenReturn(10);
        when(pipelineConfigGroup.health().readiness().periodSeconds()).thenReturn(10);
        when(pipelineConfigGroup.health().readiness().timeoutSeconds()).thenReturn(10);
        when(pipelineConfigGroup.health().readiness().failureThreshold()).thenReturn(3);

        pipelineMapper = createMapper();
    }

    @Test
    public void testMapper_ShouldUseNamesForSqlServer() {
        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.SQLSERVER, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        assertThat(result.getSpec().getSource().getConfig().getProps())
                .containsEntry("database.names", "customers")
                .containsEntry("database.user", "sa");
    }

    @Test
    public void testMapper_ShouldUseDbNameForPostgreSql() {
        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        assertThat(result.getSpec().getSource().getConfig().getProps())
                .containsEntry("database.dbname", "customers")
                .containsEntry("database.user", "sa");
    }

    @Test
    public void testMapper_ShouldMergeConfiguredLabelsWithConductorLabel() {
        when(pipelineConfigGroup.labels()).thenReturn(Map.of("argocd.argoproj.io/instance", "debezium-platform"));

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));
        when(pipeline.getName()).thenReturn("pipeline-a");

        var result = pipelineMapper.map(pipeline);

        assertThat(result.getMetadata().getLabels())
                .containsEntry("argocd.argoproj.io/instance", "debezium-platform")
                .containsEntry(LABEL_DBZ_CONDUCTOR_ID, "1");
    }

    @Test
    public void testMapper_ShouldNotGiveWorkloadIdentityWhenVaultDisabled() {
        when(pipelineConfigGroup.vault().enabled()).thenReturn(false);

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "customers"));
        when(pipeline.getName()).thenReturn("pipeline-a");

        var runtime = pipelineMapper.map(pipeline).getSpec().getRuntime();

        // Leaving serviceAccount unset is what makes the operator create and own one, which is the
        // behaviour every existing deployment relies on.
        assertThat(runtime.getServiceAccount()).isNull();
        assertThat(runtime.getStorage().getExternal()).isEmpty();
    }

    @Test
    public void testMapper_ShouldGiveEachPipelineItsOwnIdentityWhenVaultEnabled() {
        when(pipelineConfigGroup.vault().enabled()).thenReturn(true);
        when(pipelineConfigGroup.vault().audience()).thenReturn("openbao");
        when(pipelineConfigGroup.vault().volumeName()).thenReturn("openbao-token");
        when(pipelineConfigGroup.vault().tokenExpirationSeconds()).thenReturn(600L);

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "customers"));
        when(pipeline.getName()).thenReturn("pipeline-a");

        var runtime = pipelineMapper.map(pipeline).getSpec().getRuntime();

        assertThat(runtime.getServiceAccount()).isEqualTo("pipeline-a-sa");
        assertThat(runtime.getStorage().getExternal()).hasSize(1);

        var volume = runtime.getStorage().getExternal().getFirst();
        assertThat(volume.getName()).isEqualTo("openbao-token");

        var token = volume.getProjected().getSources().getFirst().getServiceAccountToken();
        // The audience is what stops a token issued for the secret backend being replayed against
        // the Kubernetes API server, so an unset one is a security regression rather than a default.
        assertThat(token.getAudience()).isEqualTo("openbao");
        assertThat(token.getPath()).isEqualTo("token");
        assertThat(token.getExpirationSeconds()).isEqualTo(600L);
    }

    @Test
    public void testMapper_ShouldKeepEphemeralDataStorageWhenVaultEnabled() {
        when(pipelineConfigGroup.vault().enabled()).thenReturn(true);
        when(pipelineConfigGroup.vault().audience()).thenReturn("openbao");
        when(pipelineConfigGroup.vault().volumeName()).thenReturn("openbao-token");
        when(pipelineConfigGroup.vault().tokenExpirationSeconds()).thenReturn(600L);

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "customers"));
        when(pipeline.getName()).thenReturn("pipeline-a");

        var storage = pipelineMapper.map(pipeline).getSpec().getRuntime().getStorage();

        // Adding an external volume must not silently drop the data storage the pipeline already
        // had; it is a sibling field on the same object.
        assertThat(storage.getData()).isNotNull();
    }

    @Test
    public void testMapper_ShouldKeepProbeTemplatesWhenVaultEnabled() {
        when(pipelineConfigGroup.vault().enabled()).thenReturn(true);
        when(pipelineConfigGroup.vault().audience()).thenReturn("openbao");
        when(pipelineConfigGroup.vault().volumeName()).thenReturn("openbao-token");
        when(pipelineConfigGroup.vault().tokenExpirationSeconds()).thenReturn(600L);

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "customers"));
        when(pipeline.getName()).thenReturn("pipeline-a");

        var runtime = pipelineMapper.map(pipeline).getSpec().getRuntime();

        // Vault identity and health probes are built by the same createRuntime; adding the one must
        // not drop the other.
        assertThat(runtime.getServiceAccount()).isEqualTo("pipeline-a-sa");
        assertThat(runtime.getTemplates().getContainer().getProbes().getLiveness()).isNotNull();
    }

    @Test
    public void testMapper_ShouldEmitCoordinatesForEachBoundVault() {
        enableVaultWithAddress();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "${vault::ecommerce/username}",
                "password", "${vault::ecommerce/password}"));
        when(pipeline.getName()).thenReturn("pipeline-a");
        var ecommerce = mockVault("ecommerce", "db/ecommerce/creds/pipeline");
        var kafka = mockVault("kafka", "secret/data/debezium/demo/kafka");
        when(pipeline.getSource().getVaults()).thenReturn(Set.of(ecommerce));
        when(pipeline.getDestination().getVaults()).thenReturn(Set.of(kafka));

        var spec = pipelineMapper.map(pipeline).getSpec();

        // The reference the admin stored on the connection reaches the resource untouched: the
        // mapper no longer decides which vault a credential comes from, the binding does.
        assertThat(spec.getSource().getConfig().getProps())
                .containsEntry("database.user", "${vault::ecommerce/username}")
                .containsEntry("database.password", "${vault::ecommerce/password}");

        // One set of coordinates per bound vault; address and role are the platform's, the path is
        // the vault's own.
        assertThat(spec.getRuntime().getEnvironment().getVars())
                .extracting("name", "value")
                .containsExactly(
                        tuple("DEBEZIUM_VAULT_NAMES", "ecommerce,kafka"),
                        tuple("DEBEZIUM_VAULT_ECOMMERCE_ADDRESS", "http://openbao.openbao.svc:8200"),
                        tuple("DEBEZIUM_VAULT_ECOMMERCE_PATH", "db/ecommerce/creds/pipeline"),
                        tuple("DEBEZIUM_VAULT_ECOMMERCE_AUTH_ROLE", "pipeline"),
                        tuple("DEBEZIUM_VAULT_ECOMMERCE_AUTH_TOKEN_PATH", "/debezium/external/openbao-token/token"),
                        tuple("DEBEZIUM_VAULT_KAFKA_ADDRESS", "http://openbao.openbao.svc:8200"),
                        tuple("DEBEZIUM_VAULT_KAFKA_PATH", "secret/data/debezium/demo/kafka"),
                        tuple("DEBEZIUM_VAULT_KAFKA_AUTH_ROLE", "pipeline"),
                        tuple("DEBEZIUM_VAULT_KAFKA_AUTH_TOKEN_PATH", "/debezium/external/openbao-token/token"));
    }

    @Test
    public void testMapper_ShouldEmitOnlyThisPipelinesVaults() {
        enableVaultWithAddress();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "inventory"));
        when(pipeline.getName()).thenReturn("pipeline-b");
        var inventory = mockVault("inventory", "db/inventory/creds/pipeline");
        var kafka = mockVault("kafka", "secret/data/debezium/demo/kafka");
        when(pipeline.getSource().getVaults()).thenReturn(Set.of(inventory));
        when(pipeline.getDestination().getVaults()).thenReturn(Set.of(kafka));

        var vars = pipelineMapper.map(pipeline).getSpec().getRuntime().getEnvironment().getVars();

        // The coordinates follow the binding: a second database is a second vault row and
        // nothing platform-wide, so a pipeline bound to it must not hear about the first.
        assertThat(vars)
                .extracting("name", "value")
                .contains(tuple("DEBEZIUM_VAULT_NAMES", "inventory,kafka"))
                .noneMatch(t -> t.toString().contains("ECOMMERCE"));
    }

    @Test
    public void testMapper_ShouldSkipBoundVaultWithoutPath() {
        enableVaultWithAddress();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "customers"));
        when(pipeline.getName()).thenReturn("pipeline-a");
        var ecommerce = mockVault("ecommerce", "db/ecommerce/creds/pipeline");
        var halfRegistered = mockVault("half-registered", null);
        when(pipeline.getSource().getVaults()).thenReturn(Set.of(ecommerce, halfRegistered));

        var vars = pipelineMapper.map(pipeline).getSpec().getRuntime().getEnvironment().getVars();

        assertThat(vars)
                .extracting("name", "value")
                .contains(tuple("DEBEZIUM_VAULT_NAMES", "ecommerce"))
                .noneMatch(t -> t.toString().contains("HALF-REGISTERED"));
    }

    @Test
    public void testMapper_ShouldEmitNoCoordinatesWhenNothingIsBound() {
        enableVaultWithAddress();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "a-stored-username"));
        when(pipeline.getName()).thenReturn("pipeline-a");

        var spec = pipelineMapper.map(pipeline).getSpec();

        // Identity without resolution is a deliberate intermediate state: the pod can prove who it
        // is, while credentials still come from the stored connection.
        assertThat(spec.getRuntime().getServiceAccount()).isEqualTo("pipeline-a-sa");
        assertThat(spec.getRuntime().getEnvironment().getVars()).isNullOrEmpty();
        assertThat(spec.getSource().getConfig().getProps()).containsEntry("database.user", "a-stored-username");
    }

    @Test
    public void testMapper_ShouldEmitNoCoordinatesWhenVaultHasNoAddress() {
        when(pipelineConfigGroup.vault().enabled()).thenReturn(true);
        when(pipelineConfigGroup.vault().audience()).thenReturn("openbao");
        when(pipelineConfigGroup.vault().volumeName()).thenReturn("openbao-token");
        when(pipelineConfigGroup.vault().tokenExpirationSeconds()).thenReturn(600L);
        when(pipelineConfigGroup.vault().address()).thenReturn(Optional.empty());

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(DATABASE, "customers"));
        when(pipeline.getName()).thenReturn("pipeline-a");
        var ecommerce = mockVault("ecommerce", "db/ecommerce/creds/pipeline");
        when(pipeline.getSource().getVaults()).thenReturn(Set.of(ecommerce));

        var spec = pipelineMapper.map(pipeline).getSpec();

        // A binding without a store to reach is not an error here; the platform half is missing,
        // and that is the operator's to supply.
        assertThat(spec.getRuntime().getServiceAccount()).isEqualTo("pipeline-a-sa");
        assertThat(spec.getRuntime().getEnvironment().getVars()).isNullOrEmpty();
    }

    @Test
    public void testResolveSinkType_ShouldReturnShortNameAlreadyShort() {
        assertThat(PipelineMapper.resolveSinkType("kafka")).isEqualTo("kafka");
        assertThat(PipelineMapper.resolveSinkType("kinesis")).isEqualTo("kinesis");
    }

    @Test
    public void testResolveSinkType_ShouldExtractFromFqcn() {
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.kafka.KafkaChangeConsumer")).isEqualTo("kafka");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.kinesis.KinesisChangeConsumer")).isEqualTo("kinesis");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.eventhubs.EventHubsChangeConsumer")).isEqualTo("eventhubs");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.http.HttpChangeConsumer")).isEqualTo("http");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.infinispan.InfinispanSinkConsumer")).isEqualTo("infinispan");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.instructlab.InstructLabSinkConsumer")).isEqualTo("instructlab");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.jdbc.JdbcChangeConsumer")).isEqualTo("jdbc");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.milvus.MilvusChangeConsumer")).isEqualTo("milvus");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.pravega.PravegaChangeConsumer")).isEqualTo("pravega");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.pubsub.PubSubChangeConsumer")).isEqualTo("pubsub");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.pulsar.PulsarChangeConsumer")).isEqualTo("pulsar");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.qdrant.QdrantChangeConsumer")).isEqualTo("qdrant");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.rabbitmq.RabbitMqStreamChangeConsumer")).isEqualTo("rabbitmq");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.redis.RedisStreamChangeConsumer")).isEqualTo("redis");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.rocketmq.RocketMqChangeConsumer")).isEqualTo("rocketmq");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.sns.SnsChangeConsumer")).isEqualTo("sns");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.sqs.SqsChangeConsumer")).isEqualTo("sqs");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.fluss.FlussChangeConsumer")).isEqualTo("fluss");
    }

    @Test
    public void testResolveSinkType_ShouldHandleOverrides() {
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.nats.jetstream.NatsJetStreamChangeConsumer")).isEqualTo("nats-jetstream");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.nats.streaming.NatsStreamingChangeConsumer")).isEqualTo("nats-streaming");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.pubsub.PubSubLiteChangeConsumer")).isEqualTo("pubsublite");
        assertThat(PipelineMapper.resolveSinkType("io.debezium.server.rabbitmq.RabbitMqStreamNativeChangeConsumer")).isEqualTo("rabbitmqstream");
    }

    @Test
    public void testResolveSinkType_ShouldHandleNullAndUnknown() {
        assertThat(PipelineMapper.resolveSinkType(null)).isNull();
        assertThat(PipelineMapper.resolveSinkType("com.custom.MySink")).isEqualTo("com.custom.MySink");
    }

    @Test
    public void testMapper_ShouldEnableOpenTelemetryWhenConfigured() {
        when(pipelineConfigGroup.monitoring().otel().enabled()).thenReturn(true);
        when(pipelineConfigGroup.monitoring().otel().endpoint()).thenReturn(Optional.of("http://otel-collector:4318"));
        pipelineMapper = createMapper();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        var otel = result.getSpec().getRuntime().getMetrics().getOpenTelemetry();
        assertThat(otel.isEnabled()).isTrue();
        assertThat(otel.getCollector().getEndpoint()).isEqualTo("http://otel-collector:4318");
    }

    @Test
    public void testMapper_ShouldNotEnableOpenTelemetryWhenDisabled() {
        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        var otel = result.getSpec().getRuntime().getMetrics().getOpenTelemetry();
        assertThat(otel.isEnabled()).isFalse();
    }

    @Test
    public void testMapper_ShouldEnableOpenTelemetryWithoutEndpointWhenNotConfigured() {
        when(pipelineConfigGroup.monitoring().otel().enabled()).thenReturn(true);
        when(pipelineConfigGroup.monitoring().otel().endpoint()).thenReturn(Optional.empty());
        pipelineMapper = createMapper();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        var otel = result.getSpec().getRuntime().getMetrics().getOpenTelemetry();
        assertThat(otel.isEnabled()).isTrue();
        assertThat(otel.getCollector().getEndpoint()).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2168")
    public void testMapper_ShouldSetOtelIntervalsOnCollectorWhenEnabled() {
        when(pipelineConfigGroup.monitoring().otel().enabled()).thenReturn(true);
        when(pipelineConfigGroup.monitoring().otel().endpoint()).thenReturn(Optional.of("http://otel-collector:4318"));
        when(pipelineConfigGroup.monitoring().otel().jmxIntervalMs()).thenReturn(2000);
        when(pipelineConfigGroup.monitoring().otel().metricExportIntervalMs()).thenReturn(10000);
        pipelineMapper = createMapper();

        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        var collector = result.getSpec().getRuntime().getMetrics().getOpenTelemetry().getCollector();
        assertThat(collector.getJmxIntervalMs()).isEqualTo(2000);
        assertThat(collector.getMetricExportIntervalMs()).isEqualTo(10000);
    }

    @Test
    @FixFor("debezium/dbz#2168")
    public void testMapper_ShouldNotSetOtelIntervalsWhenDisabled() {
        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var result = pipelineMapper.map(pipeline);

        var otel = result.getSpec().getRuntime().getMetrics().getOpenTelemetry();
        assertThat(otel.isEnabled()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2112")
    public void testMapper_ShouldHandleTransformWithEmptyPredicate() {
        var pipeline = mockPipelineWithSource(ConnectionEntity.Type.POSTGRESQL, Map.of(
                DATABASE, "customers",
                USERNAME, "sa"));

        var predicate = mock(io.debezium.platform.domain.views.Predicate.class);
        when(predicate.getType()).thenReturn(null);
        when(predicate.getConfig()).thenReturn(null);
        when(predicate.isNegate()).thenReturn(false);

        var transform = mock(Transform.class);
        when(transform.getId()).thenReturn(1L);
        when(transform.getType()).thenReturn("io.debezium.transforms.ExtractNewRecordState");
        when(transform.getConfig()).thenReturn(Map.of("delete.handling.mode", "none"));
        when(transform.getPredicate()).thenReturn(predicate);
        when(pipeline.getTransforms()).thenReturn(List.of(transform));

        var result = pipelineMapper.map(pipeline);

        assertThat(result.getSpec().getTransforms()).hasSize(1);
        assertThat(result.getSpec().getTransforms().get(0).getType())
                .isEqualTo("io.debezium.transforms.ExtractNewRecordState");
        assertThat(result.getSpec().getPredicates()).isEmpty();
    }

    private void enableVaultWithAddress() {
        when(pipelineConfigGroup.vault().enabled()).thenReturn(true);
        when(pipelineConfigGroup.vault().audience()).thenReturn("openbao");
        when(pipelineConfigGroup.vault().volumeName()).thenReturn("openbao-token");
        when(pipelineConfigGroup.vault().tokenExpirationSeconds()).thenReturn(600L);
        when(pipelineConfigGroup.vault().authRole()).thenReturn("pipeline");
        when(pipelineConfigGroup.vault().address()).thenReturn(Optional.of("http://openbao.openbao.svc:8200"));
    }

    /**
     * A bound vault: the reference a component carries (id only, as it arrives through the
     * outbox) and the row the mapper reads for it.
     */
    private VaultReference mockVault(String name, String path) {
        long id = name.hashCode() & 0xffff;
        var reference = mock(VaultReference.class);
        when(reference.getId()).thenReturn(id);

        var row = mock(Vault.class);
        when(row.getName()).thenReturn(name);
        when(row.getItems()).thenReturn(path == null ? Map.of("keys", "username,password") : Map.of("path", path, "keys", "username,password"));
        when(vaultService.findById(id)).thenReturn(Optional.of(row));

        return reference;
    }

    private PipelineMapper createMapper() {
        return new PipelineMapper(pipelineConfigGroup, tableNameResolver, buildMetrics(pipelineConfigGroup), vaultService);
    }

    private static Metrics buildMetrics(PipelineConfigGroup config) {
        var metricsBuilder = new MetricsBuilder();
        var otelStrategy = new OpenTelemetryExporterStrategy();
        if (otelStrategy.isApplicable(config)) {
            otelStrategy.apply(metricsBuilder, config);
        }
        return metricsBuilder.build();
    }

    private PipelineFlat mockPipelineWithSource(ConnectionEntity.Type type, Map<String, Object> connectionConfig) {
        var pipeline = mock(PipelineFlat.class);
        var source = mock(SourceFlat.class);
        var destination = mock(DestinationFlat.class);
        var connection = mock(Connection.class);

        when(connection.getType()).thenReturn(type);
        when(connection.getConfig()).thenReturn(connectionConfig);

        when(source.getConnection()).thenReturn(connection);

        when(pipeline.getSource()).thenReturn(source);
        when(pipeline.getDestination()).thenReturn(destination);
        when(pipeline.getDefaultLogLevel()).thenReturn("INFO");
        when(pipeline.getLogLevels()).thenReturn(Map.of());
        when(pipeline.getId()).thenReturn(1L);

        return pipeline;
    }
}
