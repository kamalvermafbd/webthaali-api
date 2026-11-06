"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { createConnectorAuthenticationBoundary } = require("./connectorAuthenticationBoundary.cjs");
const { createTallyResourceBindingResolver } = require("./tallyResourceBindingResolver.cjs");
function fixture(options = {}) {
    let time = 100, active = true, checks = 0, verifications = 0;
    const credential = Object.freeze({});
    const verified = { authenticated: true, installationId: "installation", credentialId: "credential",
        credentialVersion: "1", bindingVersion: "3", expiresAt: 200 };
    const boundary = createConnectorAuthenticationBoundary({
        verifyCredential: options.verifyCredential || (async input => {
            verifications++;
            return input.credential === credential ? verified : null;
        }),
        isCredentialCurrent: options.isCredentialCurrent || (() => { checks++; return active; }),
        now: () => time
    });
    const session = Object.freeze({});
    boundary.openSession(session);
    return { boundary, session, credential, verified,
        login: () => boundary.authenticate({ session, credential }),
        lookup: boundary.forSession(session),
        expire() { time = 200; }, revokeCredential() { active = false; },
        get checks() { return checks; }, get verifications() { return verifications; } };
}
test("valid credential yields exact-session frozen principal and evidence", async () => {
    const f = fixture(), result = await f.login();
    assert.equal(result.status, "AUTHENTICATED");
    assert.equal(f.lookup(result.evidence), result.principal);
    assert.equal(result.principal.installationId, "installation");
    assert.equal(result.principal.credentialId, "credential");
    assert.equal(result.principal.credentialVersion, "1");
    assert.equal(result.principal.bindingVersion, "3");
    assert.equal(result.principal.sessionGeneration, "1");
    assert.ok(Object.isFrozen(result)); assert.ok(Object.isFrozen(result.evidence));
    assert.ok(Object.isFrozen(result.principal)); assert.deepEqual(Object.keys(result.evidence), []);
    assert.equal(f.checks, 2);
});
test("missing credentials never call verifier", async () => {
    const f = fixture();
    for (const credential of [undefined, null, ""]) {
        assert.equal((await f.boundary.authenticate({ session: f.session, credential })).status, "UNAUTHENTICATED");
    }
    assert.equal(f.verifications, 0);
});
test("invalid and forged credentials or trusted identity claims do not authenticate", async () => {
    const f = fixture();
    for (const credential of ["installation", { trusted: true, installationId: "installation",
        resourceId: "resource", connectorId: "alias", companyGuid: "company" }]) {
        const result = await f.boundary.authenticate({ session: f.session, credential, trusted: true });
        assert.equal(result.status, "UNAUTHENTICATED"); assert.equal(result.evidence, undefined);
    }
});
test("unregistered session cannot authenticate", async () => {
    const f = fixture();
    assert.equal((await f.boundary.authenticate({ session: {}, credential: f.credential })).reason, "SESSION_UNAVAILABLE");
    assert.equal(f.verifications, 0);
});
test("expired credentials rejected on issuance and lookup", async () => {
    const f = fixture(), r = await f.login(); f.expire();
    assert.equal(f.lookup(r.evidence), null);
    assert.equal((await f.login()).reason, "CREDENTIAL_NOT_CURRENT");
});
test("revocation rechecked on every lookup and invalidates old evidence", async () => {
    const f = fixture(), r = await f.login(); f.revokeCredential();
    assert.equal(f.lookup(r.evidence), null); assert.equal(f.lookup(r.evidence), null);
    assert.equal((await f.login()).status, "UNAUTHENTICATED");
});
test("explicit session revocation denies evidence and reauthentication", async () => {
    const f = fixture(), r = await f.login();
    assert.equal(f.boundary.revokeSession(f.session), true);
    assert.equal(f.lookup(r.evidence), null);
    assert.equal((await f.login()).reason, "SESSION_UNAVAILABLE");
});
test("replacement invalidates replay and changes generation", async () => {
    const f = fixture(), r = await f.login(), next = {};
    const metadata = f.boundary.replaceSession(f.session, next);
    assert.equal(metadata.sessionGeneration, "2");
    assert.equal(f.lookup(r.evidence), null);
    assert.equal(f.boundary.forSession(next)(r.evidence), null);
    const newer = await f.boundary.authenticate({ session: next, credential: f.credential });
    assert.equal(newer.status, "AUTHENTICATED");
    assert.equal(newer.principal.sessionGeneration, "2");
});
test("evidence cannot authorize another registered session", async () => {
    const f = fixture(), r = await f.login(), other = {};
    f.boundary.openSession(other);
    assert.equal(f.boundary.forSession(other)(r.evidence), null);
    assert.equal(f.lookup(r.evidence), r.principal);
});
test("copied fields and fresh empty objects are not evidence", async () => {
    const f = fixture(), r = await f.login();
    for (const fake of [{}, { ...r.evidence }, { ...r.principal, trusted: true }, "installation"]) {
        assert.equal(f.lookup(fake), null);
    }
});
test("caller and verifier mutations cannot change issued principal", async () => {
    const f = fixture(), r = await f.login();
    f.verified.installationId = "changed";
    assert.equal(f.lookup(r.evidence).installationId, "installation");
    assert.throws(() => { r.principal.installationId = "spoof"; }, TypeError);
    assert.throws(() => { r.evidence.trusted = true; }, TypeError);
    assert.equal(f.session.installationId, undefined);
});
test("missing trusted dependencies reject construction", () => {
    assert.throws(() => createConnectorAuthenticationBoundary(), TypeError);
    assert.throws(() => createConnectorAuthenticationBoundary({ verifyCredential() {} }), TypeError);
});
for (const mode of ["throw", "reject", "malformed", "false"]) {
    test("verifier " + mode + " fails closed", async () => {
        const f = fixture({ verifyCredential() {
            if (mode === "throw") throw Error("offline");
            if (mode === "reject") return Promise.reject(Error("offline"));
            return mode === "false" ? { authenticated: false } : { authenticated: true, installationId: "i" };
        } });
        const r = await f.login();
        assert.equal(r.status, "UNAUTHENTICATED"); assert.equal(r.evidence, undefined);
    });
}
test("current authority unavailable or async never authorizes", async () => {
    for (const check of [() => { throw Error("offline"); }, () => Promise.reject(Error("offline")), () => "true"]) {
        const f = fixture({ isCredentialCurrent: check });
        assert.equal((await f.login()).status, "UNAUTHENTICATED");
    }
});
test("pending verification cannot outlive session replacement", async () => {
    let release;
    const pending = new Promise(resolve => { release = resolve; });
    const f = fixture({ verifyCredential: () => pending });
    const auth = f.login(); f.boundary.replaceSession(f.session, {});
    release(f.verified);
    assert.equal((await auth).reason, "SESSION_REPLACED");
});
test("later authentication attempt supersedes earlier result", async () => {
    let release;
    const pending = new Promise(resolve => { release = resolve; });
    const f = fixture({ verifyCredential: () => pending });
    const first = f.login(), second = f.login(); release(f.verified);
    assert.equal((await first).status, "UNAUTHENTICATED");
    assert.equal((await second).status, "AUTHENTICATED");
});
test("resource resolver consumes only exact-session evidence and trusted mapping", async () => {
    const f = fixture(), r = await f.login();
    const mapping = { aliases: [{ connectorAlias: "alias", installationId: "installation" }],
        installations: [{ installationId: "installation", resourceId: "tally", bindingVersion: "3" }] };
    const resolve = createTallyResourceBindingResolver({ getProvisionedBindings: () => mapping,
        resolveAuthenticatedInstallation: f.lookup });
    const input = { connectorAlias: "alias", authenticationEvidence: r.evidence, resourceId: "spoof" };
    assert.equal(resolve(input).resourceId, "tally");
    assert.equal(resolve({ ...input, authenticationEvidence: { ...r.principal } }).status, "UNRESOLVED");
    mapping.installations[0].bindingVersion = "4";
    assert.equal(resolve(input).reason, "STALE_BINDING");
    f.boundary.revokeSession(f.session);
    assert.equal(resolve(input).reason, "UNAUTHENTICATED");
});

test("failed reauthentication immediately invalidates previously valid evidence", async () => {
    let calls = 0, rejectReauthentication;
    const pendingFailure = new Promise(resolve => { rejectReauthentication = resolve; });
    const f = fixture({ verifyCredential: () => ++calls === 1 ? f.verified : pendingFailure });
    const first = await f.login();
    assert.equal(first.status, "AUTHENTICATED");
    assert.equal(f.lookup(first.evidence), first.principal);
    const retry = f.login();
    assert.equal(calls, 2);
    assert.equal(f.lookup(first.evidence), null, "Old evidence must be invalid before verifier settles");
    rejectReauthentication({ authenticated: false });
    const failed = await retry;
    assert.equal(failed.status, "UNAUTHENTICATED");
    assert.equal(failed.evidence, undefined);
    assert.equal(f.lookup(first.evidence), null, "Failed attempt must not restore old authorization");
});

test("synchronous current-authority revocation during lookup invalidates evidence", async () => {
    let revokeDuringLookup = false, checks = 0;
    const f = fixture({ isCredentialCurrent: () => {
        checks++;
        if (revokeDuringLookup) {
            assert.equal(f.boundary.revokeSession(f.session), true);
        }
        return true;
    } });
    const first = await f.login();
    assert.equal(first.status, "AUTHENTICATED");
    revokeDuringLookup = true;
    assert.equal(f.lookup(first.evidence), null);
    const checksAfterRevocation = checks;
    revokeDuringLookup = false;
    assert.equal(f.lookup(first.evidence), null);
    assert.equal(checks, checksAfterRevocation, "Invalid evidence must not reach authority again");
    assert.equal((await f.login()).reason, "SESSION_UNAVAILABLE");
});

test("explicit revoke while verifier pending rejects late successful verification", async () => {
    let finishVerification, verifierCalls = 0;
    const pending = new Promise(resolve => { finishVerification = resolve; });
    const f = fixture({ verifyCredential: () => { verifierCalls++; return pending; } });
    const authentication = f.login();
    assert.equal(verifierCalls, 1);
    assert.equal(f.boundary.revokeSession(f.session), true);
    finishVerification(f.verified);
    const result = await authentication;
    assert.equal(result.status, "UNAUTHENTICATED");
    assert.equal(result.reason, "SESSION_REPLACED");
    assert.equal(result.evidence, undefined);
    assert.equal((await f.login()).reason, "SESSION_UNAVAILABLE");
});

test("revoked session object cannot be reopened or resurrect evidence", async () => {
    const f = fixture(), first = await f.login();
    assert.equal(first.status, "AUTHENTICATED");
    assert.equal(f.boundary.revokeSession(f.session), true);
    assert.throws(() => f.boundary.openSession(f.session), /Session object already registered/);
    assert.equal(f.lookup(first.evidence), null);
    assert.equal((await f.login()).reason, "SESSION_UNAVAILABLE");
});

test("newer independent verification resolves first and older result cannot overwrite it", async () => {
    let finishOlder, finishNewer, calls = 0;
    const olderGate = new Promise(resolve => { finishOlder = resolve; });
    const newerGate = new Promise(resolve => { finishNewer = resolve; });
    const f = fixture({ verifyCredential: () => ++calls === 1 ? olderGate : newerGate });
    const olderAttempt = f.login();
    const newerAttempt = f.login();
    assert.equal(calls, 2);
    finishNewer({ ...f.verified, credentialId: "newer-credential" });
    const newer = await newerAttempt;
    assert.equal(newer.status, "AUTHENTICATED");
    assert.equal(newer.principal.credentialId, "newer-credential");
    assert.equal(f.lookup(newer.evidence), newer.principal);
    finishOlder({ ...f.verified, credentialId: "older-credential" });
    const older = await olderAttempt;
    assert.equal(older.status, "UNAUTHENTICATED");
    assert.equal(older.reason, "SESSION_REPLACED");
    assert.equal(older.evidence, undefined);
    assert.equal(f.lookup(newer.evidence), newer.principal, "Late older completion must leave newer evidence valid");
});
