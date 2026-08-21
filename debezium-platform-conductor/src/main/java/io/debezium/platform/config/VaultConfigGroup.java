/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.platform.config;

import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Configuration group for the workload identity a pipeline pod presents to a secret backend.
 * <p>
 * When enabled, each generated {@code DebeziumServer} runs as its own ServiceAccount and mounts a
 * projected token scoped to {@link #audience()}. The pod exchanges that token for credentials by
 * calling the backend itself; the conductor never holds a credential and none is written to the
 * custom resource or to a ConfigMap.
 * </p>
 * <p>
 * The audience is what contains blast radius. A token stamped for the secret backend cannot be
 * replayed against the Kubernetes API server, and omitting it would silently default the token
 * back to the API server audience.
 * </p>
 */
public interface VaultConfigGroup {

    /**
     * Indicates whether pipeline pods are given a workload identity.
     * <p>
     * Disabled by default: enabling it makes the conductor create a ServiceAccount per pipeline,
     * which requires additional RBAC.
     * </p>
     *
     * @return {@code true} if pipelines should carry a projected backend token
     */
    @WithDefault("false")
    boolean enabled();

    /**
     * Returns the audience claim requested for the projected ServiceAccount token.
     * <p>
     * Must match the audience the secret backend's Kubernetes auth role is bound to, or the
     * backend rejects the login with an error that does not obviously point at the audience.
     * </p>
     *
     * @return the {@code aud} claim to request
     */
    @WithDefault("openbao")
    String audience();

    /**
     * Returns the name of the projected volume carrying the token.
     * <p>
     * The operator mounts external volumes at {@code /debezium/external/<name>}, so this also
     * determines the path the token appears at inside the pod. The mount path itself is not
     * configurable through the custom resource.
     * </p>
     *
     * @return the volume name
     */
    @WithDefault("openbao-token")
    @WithName("volume-name")
    String volumeName();

    /**
     * Returns the requested lifetime of the projected token, in seconds.
     * <p>
     * The kubelet rotates the token before expiry, so this bounds how long a leaked token remains
     * usable rather than how long the pipeline runs.
     * </p>
     *
     * @return the requested token lifetime in seconds
     */
    @WithDefault("600")
    @WithName("token-expiration-seconds")
    long tokenExpirationSeconds();
}
