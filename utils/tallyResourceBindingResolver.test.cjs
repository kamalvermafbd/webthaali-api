"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { createTallyResourceBindingResolver } = require("./tallyResourceBindingResolver.cjs");
function fixture() {
    const mapping = {
        aliases: [
            { connectorAlias: "a", installationId: "i" },
            { connectorAlias: "b", installationId: "i" },
            { connectorAlias: "c", installationId: "j" }
        ],
        installations: [
            { installationId: "i", resourceId: "tally", bindingVersion: "1" },
            { installationId: "j", resourceId: "tally", bindingVersion: "7" }
        ]
    };
    const identities = new WeakMap();
    const evidence = Object.freeze({});
    const otherEvidence = Object.freeze({});
    identities.set(evidence, { installationId: "i", bindingVersion: "1" });
    identities.set(otherEvidence, { installationId: "j", bindingVersion: "7" });
    const resolve = createTallyResourceBindingResolver({
        getProvisionedBindings: () => mapping,
        resolveAuthenticatedInstallation: value => identities.get(value) || null
    });
    const input = { connectorAlias: "a", authenticationEvidence: evidence };
    return { mapping, resolve, input, evidence, otherEvidence };
}
function fails(result, reason) {
    assert.deepEqual(result, { status: "UNRESOLVED", reason });
    assert.ok(Object.isFrozen(result));
    assert.equal(Object.hasOwn(result, "resourceId"), false);
}
test("aliases and independent authenticated sockets share canonical resource", () => {
    const f = fixture();
    for (const input of [f.input, { ...f.input, connectorAlias: "b", socketId: "socket2" },
        { connectorAlias: "c", authenticationEvidence: f.otherEvidence, socketId: "socket3" }]) {
        assert.equal(f.resolve(input).resourceId, "tally");
    }
});
test("unknown alias never generates resource", () => {
    const f = fixture(); fails(f.resolve({ ...f.input, connectorAlias: "unknown" }), "UNKNOWN_ALIAS");
});
test("resource, endpoint, hostname and company claims have no authority", () => {
    const f = fixture();
    assert.deepEqual(f.resolve({ ...f.input, resourceId: "evil", hostname: "evil",
        endpointUrl: "http://evil", companyGuid: "evil", trusted: true }), f.resolve(f.input));
});
test("installation string and trusted flag cannot authenticate", () => {
    const f = fixture();
    for (const authenticationEvidence of ["i", { installationId: "i", trusted: true }, {}, null]) {
        fails(f.resolve({ ...f.input, authenticationEvidence }), "UNAUTHENTICATED");
    }
});
test("authenticated installation must match alias", () => {
    const f = fixture(); fails(f.resolve({ ...f.input, authenticationEvidence: f.otherEvidence }),
        "INSTALLATION_MISMATCH");
});
test("replaced installation mapping rejects old authenticated session", () => {
    const f = fixture(); f.mapping.aliases[0].installationId = "j";
    fails(f.resolve(f.input), "INSTALLATION_MISMATCH");
});
test("resource rebinding with changed version rejects stale evidence", () => {
    const f = fixture(); f.mapping.installations[0].resourceId = "replacement";
    f.mapping.installations[0].bindingVersion = "2";
    fails(f.resolve(f.input), "STALE_BINDING");
});
test("conflicting and identical duplicate aliases are ambiguous", () => {
    for (const installationId of ["i", "j"]) {
        const f = fixture(); f.mapping.aliases.push({ connectorAlias: "a", installationId });
        fails(f.resolve(f.input), "AMBIGUOUS_ALIAS");
    }
});
test("duplicate installation bindings reject even when equal", () => {
    for (const resourceId of ["tally", "other"]) {
        const f = fixture(); f.mapping.installations.push({ installationId: "i", resourceId, bindingVersion: "1" });
        fails(f.resolve(f.input), "AMBIGUOUS_INSTALLATION");
    }
});
test("dangling installation reference fails closed", () => {
    const f = fixture(); f.mapping.installations.shift();
    fails(f.resolve(f.input), "MISSING_INSTALLATION");
});
test("malformed mappings reject without coercion", () => {
    for (const value of [null, 17, "", " i "]) {
        const f = fixture(); f.mapping.installations[0].resourceId = value;
        fails(f.resolve(f.input), "INVALID_MAPPING");
    }
});
test("malformed caller inputs reject", () => {
    const f = fixture();
    for (const value of [null, {}, "a", { connectorAlias: " a " }]) {
        fails(f.resolve(value), "INVALID_INPUT");
    }
});
test("missing mapping and provider exceptions fail closed", () => {
    for (const getProvisionedBindings of [() => null, () => { throw Error("unavailable"); }]) {
        const resolve = createTallyResourceBindingResolver({ getProvisionedBindings,
            resolveAuthenticatedInstallation: () => ({ installationId: "i", bindingVersion: "1" }) });
        const result = resolve({ connectorAlias: "a", authenticationEvidence: {} });
        assert.equal(result.status, "UNRESOLVED");
    }
});
test("async authentication provider rejects with valid synchronous mapping", () => {
    const f = fixture();
    const resolve = createTallyResourceBindingResolver({
        getProvisionedBindings: () => f.mapping,
        resolveAuthenticatedInstallation: () => Promise.resolve({
            installationId: "i", bindingVersion: "1"
        })
    });
    fails(resolve(f.input), "UNAUTHENTICATED");
});
test("async mapping provider rejects with valid synchronous authentication", () => {
    const f = fixture();
    const resolve = createTallyResourceBindingResolver({
        getProvisionedBindings: () => Promise.resolve(f.mapping),
        resolveAuthenticatedInstallation: () => ({
            installationId: "i", bindingVersion: "1"
        })
    });
    fails(resolve(f.input), "INVALID_MAPPING");
});
test("resource reassignment without version increment cannot be detected", () => {
    const f = fixture();
    assert.equal(f.resolve(f.input).resourceId, "tally");
    f.mapping.installations[0].resourceId = "replacement";
    // Provisioning contract limitation, not a security guarantee:
    // the trusted provisioning authority MUST increment bindingVersion
    // on every resource reassignment. The resolver cannot detect its omission.
    const result = f.resolve(f.input);
    assert.equal(result.status, "RESOLVED");
    assert.equal(result.resourceId, "replacement");
    assert.equal(result.bindingVersion, "1");
});
test("authentication-provider exception returns AUTHORITY_ERROR", () => {
    const f = fixture();
    const resolve = createTallyResourceBindingResolver({
        getProvisionedBindings: () => f.mapping,
        resolveAuthenticatedInstallation: () => {
            throw new Error("authentication authority unavailable");
        }
    });
    fails(resolve(f.input), "AUTHORITY_ERROR");
});
test("snapshots immutable, inputs unchanged, no cached stale binding", () => {
    const f = fixture(); const before = JSON.stringify(f.mapping);
    Object.freeze(f.input);
    const first = f.resolve(f.input);
    assert.ok(Object.isFrozen(first));
    assert.throws(() => { first.resourceId = "evil"; }, TypeError);
    assert.equal(JSON.stringify(f.mapping), before);
    f.mapping.installations[0].bindingVersion = "2";
    assert.equal(first.resourceId, "tally");
    fails(f.resolve(f.input), "STALE_BINDING");
});
test("deep-frozen provisioned input is accepted without mutation", () => {
    const f = fixture();
    for (const rows of [f.mapping.aliases, f.mapping.installations]) {
        rows.forEach(Object.freeze); Object.freeze(rows);
    }
    Object.freeze(f.mapping);
    assert.equal(f.resolve(f.input).status, "RESOLVED");
});
test("providers required rather than inferred from caller input", () => {
    assert.throws(() => createTallyResourceBindingResolver(), TypeError);
});
