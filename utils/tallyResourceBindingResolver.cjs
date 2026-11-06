"use strict";

/**
 * Isolated resource-binding resolution; NOT authentication or global exclusion.
 * No production mapping schema is assumed. Both injected synchronous providers
 * must be private server wiring, never selected or populated by request data.
 *
 * getProvisionedBindings() supplies one coherent trusted snapshot:
 * { aliases: [{ connectorAlias, installationId }],
 *   installations: [{ installationId, resourceId, bindingVersion }] }.
 * IDs/versions are opaque nonempty strings without surrounding whitespace.
 * Duplicate alias/installation entries are rejected even when identical.
 * Several aliases/installations may intentionally name one canonical resource.
 *
 * resolveAuthenticatedInstallation(evidence) must validate opaque evidence from
 * an actual trusted authentication boundary (e.g. a private WeakMap), returning
 * { installationId, bindingVersion }, or null. A string, trusted:true flag or
 * connector assertion is NOT authentication evidence. This module cannot prove
 * that an injected authority is honest. Never implement it as an identity echo.
 * The bindingVersion pins the authenticated session to current provisioning;
 * replacement/rebinding requires renewed authentication evidence.
 *
 * Connector resource claims, socket IDs, hostnames, URLs and company GUIDs are
 * ignored. Providers must be synchronous, deterministic and side-effect-free
 * for this resolver to be pure. No generated IDs, I/O, caches or runtime hooks.
 * A result is only a snapshot: subsequent revocation/rebinding requires a new
 * resolution. It authorizes neither Tally execution nor Redis lock acquisition.
 */
function createTallyResourceBindingResolver({
    getProvisionedBindings,
    resolveAuthenticatedInstallation
} = {}) {
    if (typeof getProvisionedBindings !== "function" ||
        typeof resolveAuthenticatedInstallation !== "function") {
        throw new TypeError("Trusted mapping and authentication providers required");
    }

    const unresolved = reason => Object.freeze({ status: "UNRESOLVED", reason });
    const validId = value => typeof value === "string" &&
        value.length > 0 && value.trim() === value;
    const record = value => value !== null && typeof value === "object" &&
        !Array.isArray(value) && typeof value.then !== "function";

    return function resolveResourceBinding(input) {
        try {
            if (!record(input) || !validId(input.connectorAlias)) {
                return unresolved("INVALID_INPUT");
            }
            const evidence = input.authenticationEvidence;
            if (!record(evidence)) return unresolved("UNAUTHENTICATED");
            const authenticated = resolveAuthenticatedInstallation(evidence);
            if (!record(authenticated) ||
                !validId(authenticated.installationId) ||
                !validId(authenticated.bindingVersion)) {
                return unresolved("UNAUTHENTICATED");
            }
            const mapping = getProvisionedBindings();
            if (!record(mapping) || !Array.isArray(mapping.aliases) ||
                !Array.isArray(mapping.installations)) {
                return unresolved("INVALID_MAPPING");
            }
            const aliases = new Map();
            const installations = new Map();
            for (const row of mapping.aliases) {
                if (!record(row) || !validId(row.connectorAlias) ||
                    !validId(row.installationId)) return unresolved("INVALID_MAPPING");
                if (aliases.has(row.connectorAlias)) return unresolved("AMBIGUOUS_ALIAS");
                aliases.set(row.connectorAlias, row.installationId);
            }
            for (const row of mapping.installations) {
                if (!record(row) || !validId(row.installationId) ||
                    !validId(row.resourceId) || !validId(row.bindingVersion)) {
                    return unresolved("INVALID_MAPPING");
                }
                if (installations.has(row.installationId)) {
                    return unresolved("AMBIGUOUS_INSTALLATION");
                }
                installations.set(row.installationId, {
                    resourceId: row.resourceId, bindingVersion: row.bindingVersion
                });
            }
            for (const installationId of aliases.values()) {
                if (!installations.has(installationId)) return unresolved("MISSING_INSTALLATION");
            }
            const installationId = aliases.get(input.connectorAlias);
            if (installationId === undefined) return unresolved("UNKNOWN_ALIAS");
            if (installationId !== authenticated.installationId) {
                return unresolved("INSTALLATION_MISMATCH");
            }
            const binding = installations.get(installationId);
            if (binding.bindingVersion !== authenticated.bindingVersion) {
                return unresolved("STALE_BINDING");
            }
            return Object.freeze({
                status: "RESOLVED", connectorAlias: input.connectorAlias,
                installationId, resourceId: binding.resourceId,
                bindingVersion: binding.bindingVersion
            });
        } catch {
            return unresolved("AUTHORITY_ERROR");
        }
    };
}

module.exports = { createTallyResourceBindingResolver };
