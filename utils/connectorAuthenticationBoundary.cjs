"use strict";

/**
 * Isolated authentication evidence boundary. No runtime integration or I/O.
 * ALL dependencies and lifecycle methods are trusted server wiring, never
 * request-controlled. openSession/replaceSession accept actual server-owned
 * session objects; callers must NOT open sessions from deserialized data.
 * Generations are private boundary-local counters, not distributed fencing.
 *
 * verifyCredential({credential, sessionGeneration}) is a trusted async verifier.
 * It must authenticate possession, not echo caller fields, and return:
 * { authenticated:true, installationId, credentialId, credentialVersion,
 *   bindingVersion, expiresAt } (absolute millisecond timestamp).
 * No production credential format or enrollment authority is assumed.
 *
 * isCredentialCurrent(principal) is a trusted SYNCHRONOUS check, called before
 * issuance and on EVERY lookup. Only literal true accepts. It must check current
 * revocation, credential version and provisioning binding version, failing
 * closed when its authoritative state is unavailable. Any cache freshness is
 * the integrating application's responsibility. The synchronous resolver cannot
 * perform asynchronous revocation checks: refresh trusted state separately or
 * refuse lookup. A Promise from this dependency never authorizes evidence.
 *
 * Expiry uses injected trusted monotonic/nondecreasing wall-clock time. Revoking
 * or replacing a session invalidates evidence and outstanding verifications.
 * A denied lookup permanently invalidates that session's current evidence;
 * reauthentication is required. Snapshots do not remain authorization forever.
 * forSession(session) produces the exact-session lookup for the resource
 * resolver's resolveAuthenticatedInstallation(evidence) dependency.
 * Evidence is an opaque capability: keep it private, never serialize/expose it.
 * This module establishes no resource authorization, exclusion or remote
 * settlement. No legacy unauthenticated fallback exists.
 */
function createConnectorAuthenticationBoundary({
    verifyCredential, isCredentialCurrent, now = Date.now
} = {}) {
    if (typeof verifyCredential !== "function" ||
        typeof isCredentialCurrent !== "function" || typeof now !== "function") {
        throw new TypeError("Trusted verifier, current-credential check and clock required");
    }
    const sessions = new WeakMap();
    const evidenceRecords = new WeakMap();
    let generation = 0n;
    const object = value => value !== null && typeof value === "object" && !Array.isArray(value);
    const id = value => typeof value === "string" && value.length > 0 && value.trim() === value;
    const denied = reason => Object.freeze({ status: "UNAUTHENTICATED", reason });
    function invalidate(state) {
        state.attempt++;
        if (state.evidence) evidenceRecords.delete(state.evidence);
        state.evidence = null;
    }
    function openSession(session) {
        if (!object(session)) throw new TypeError("Server-owned session object required");
        const previous = sessions.get(session);
        if (previous) throw new Error("Session object already registered; use a fresh object");
        generation++;
        const state = { active: true, generation: generation.toString(), attempt: 0, evidence: null };
        sessions.set(session, state);
        return Object.freeze({ sessionGeneration: state.generation });
    }
    function revokeSession(session) {
        const state = object(session) && sessions.get(session);
        if (!state) return false;
        state.active = false;
        invalidate(state);
        return true;
    }
    function replaceSession(oldSession, newSession) {
        if (!object(newSession) || sessions.has(newSession)) {
            throw new TypeError("Fresh server-owned replacement session required");
        }
        const old = object(oldSession) && sessions.get(oldSession);
        if (!old || !old.active) throw new Error("Active original session required");
        revokeSession(oldSession);
        return openSession(newSession);
    }
    function current(principal) {
        try {
            const time = now();
            if (!Number.isSafeInteger(time) || time < 0 || principal.expiresAt <= time) return false;
            const result = isCredentialCurrent(principal);
            if (result && typeof result.then === "function") {
                Promise.resolve(result).catch(() => {});
                return false;
            }
            return result === true;
        } catch { return false; }
    }
    async function authenticate({ session, credential } = {}) {
        const state = object(session) && sessions.get(session);
        if (!state || !state.active) return denied("SESSION_UNAVAILABLE");
        invalidate(state);
        const attempt = state.attempt;
        if (credential === undefined || credential === null || credential === "") {
            return denied("CREDENTIAL_REQUIRED");
        }
        let verified;
        let principal;
        try {
            verified = await verifyCredential(Object.freeze({ credential, sessionGeneration: state.generation }));
            if (!object(verified) || verified.authenticated !== true ||
                !id(verified.installationId) || !id(verified.credentialId) ||
                !id(verified.credentialVersion) || !id(verified.bindingVersion) ||
                !Number.isSafeInteger(verified.expiresAt)) return denied("VERIFICATION_REJECTED");
            principal = Object.freeze({
                installationId: verified.installationId, credentialId: verified.credentialId,
                credentialVersion: verified.credentialVersion, bindingVersion: verified.bindingVersion,
                expiresAt: verified.expiresAt, sessionGeneration: state.generation
            });
        } catch { return denied("VERIFIER_ERROR"); }
        if (!state.active || state.attempt !== attempt) return denied("SESSION_REPLACED");
        if (!current(principal)) return denied("CREDENTIAL_NOT_CURRENT");
        // A trusted current-check may synchronously revoke/replace the session.
        if (!state.active || state.attempt !== attempt) return denied("SESSION_REPLACED");
        const evidence = Object.freeze({});
        state.evidence = evidence;
        evidenceRecords.set(evidence, { session, state, principal });
        return Object.freeze({ status: "AUTHENTICATED", evidence, principal });
    }
    function lookup(session, evidence) {
        if (!object(session) || !object(evidence)) return null;
        const entry = evidenceRecords.get(evidence);
        const state = sessions.get(session);
        if (!entry || entry.session !== session || entry.state !== state ||
            !state.active || state.evidence !== evidence) return null;
        if (!current(entry.principal)) { invalidate(state); return null; }
        if (!state.active || state.evidence !== evidence) return null;
        return entry.principal;
    }
    function forSession(session) {
        if (!object(session) || !sessions.has(session)) throw new TypeError("Registered session required");
        return evidence => lookup(session, evidence);
    }
    return Object.freeze({ openSession, revokeSession, replaceSession, authenticate, forSession });
}
module.exports = { createConnectorAuthenticationBoundary };
