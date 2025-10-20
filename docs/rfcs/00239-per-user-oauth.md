RFC: Per-user OAuth via `backendAuth.http` (external credential broker)
Status: Draft
Related: agentgateway/agentgateway#239 (Per-user auth to external SaaS MCP backends behind SSO)
Authors: mayank6136
Reviewers: maintainers, security

1) Summary
Enterprises want users to call external SaaS MCP backends (Atlassian, GitHub, etc.) with their own credentials while authenticating to Agent Gateway via SSO (Okta). Today, `backendAuth` supports static keys, passthrough, and cloud-provider signing; it does not fetch per-user access tokens dynamically. This RFC proposes adding `backendAuth.http`, which calls an external credential broker to obtain a per-user token (derived from verified inbound JWT claims), caches it briefly, and injects it into outbound requests.
2) Problem
Issue #239 requests enterprise SSO with per-user auth to external MCP services, including first-time OAuth flows, linking/unlinking, and auditability. Current `backendAuth` options are static or cloud-provider based; there is no per-user token acquisition path.
3) Goals / Non-Goals
Goals
User-scoped tokens on outbound calls to SaaS MCP backends.
Stateless Agent Gateway for per-user tokens (no DB in AG).
Short-lived cache; templated inputs from verified JWT claims (e.g., jwt.email).
Surface “authorization required” challenges via 401/WWW-Authenticate to let clients complete OAuth.
Non-Goals (Phase 1)
Implementing a full token store/refresh inside Agent Gateway.
Building a full UI to manage linked accounts (can be proxied from the broker; optional later).
4) Current state (baseline)
`backendAuth` can attach a static key (inline or file), passthrough the inbound JWT, or use GCP/AWS signing. No per-user dynamic OAuth.
MCP/OAuth guidance recommends returning 401 with WWW-Authenticate and supporting OAuth flows; the gateway should propagate the challenge.
5) Proposed design: `backendAuth.http`
Add a new backend auth method that calls a credential broker (internal service) to obtain a per-user access token. The broker handles OAuth (Auth Code + PKCE), storage (Vault/Redis/DB), refresh, and audit. The gateway: (1) extracts verified JWT claims (e.g., sub, email), (2) calls the broker over HTTP(S), (3) parses the token, (4) caches it briefly per (backend, user), (5) injects the configured header on outbound requests, (6) if the broker signals authorization needed, returns a standards-aligned challenge so the client can complete OAuth.
5.1 Config schema (YAML)
backendAuth:
  http:
    url: "https://cred-broker.internal/token"   # required
    method: POST                                # default: POST
    headers:                                    # templates can reference verified claims
      X-Principal: "{{ jwt.email }}"
      X-Service: "atlassian"
    body: |                                     # optional; templated
      { "service": "atlassian", "user": "{{ jwt.sub }}" }
    response:
      from: "json.access_token"                 # or: "header.Authorization"
    header:
      name: "Authorization"
      format: "Bearer {token}"
    cacheTtl: "900s"                            # default: 15m
    timeoutMs: 1500
5.2 Request templating
Template values from verified inbound JWT claims: {{ jwt.email }}, {{ jwt.sub }}, {{ jwt.groups }}, etc.
Rendered in headers and optional JSON body.
5.3 Broker response contract
Success: JSON { "access_token": "<opaque>" } (or token returned in a response header). The gateway injects the configured header.
Auth needed: 401 with WWW-Authenticate and/or JSON { "error":"authorization_required", "auth_url":"<url>", "service":"<name>" }. The gateway propagates a standards-aligned challenge so MCP clients can launch OAuth.
5.4 Error handling & propagation
On broker 401, include WWW-Authenticate details in the response to the client (or map to a typed error), aligning with MCP/OAuth flows.
5.5 Caching
Async in-memory TTL cache keyed by (backend-id, broker-url, user-identity) for cacheTtl.
Cache is non-persistent; on restart the gateway re-fetches tokens.
5.6 Observability & security
Metrics: successes, failures, cache hit ratio, broker latency.
Logs: redact tokens; include correlation IDs.
Security: broker is internal, TLS required; broker stores tokens at rest. Gateway never persists per-user tokens.
6) Sequence diagrams (text)
First access (not yet authorized):

Client → AG: request to Atlassian MCP
AG → Broker: POST /token  (X-Principal=alice@corp.com, body {service:"atlassian", user:"sub-123"})
Broker → AG: 401 + WWW-Authenticate + {error:"authorization_required", auth_url:"https://auth.atlassian.com/authorize?...}
AG → Client: 401 + WWW-Authenticate (propagated)  ← client opens auth_url and completes OAuth
Subsequent access (authorized):

Client → AG
AG → Broker: POST /token (same inputs)
Broker → AG: {access_token:"ya29.a0Af..."}
AG → Atlassian MCP: Authorization: Bearer <token>
7) Alternatives considered
Built-in token service inside Agent Gateway: more control but requires persistent storage, migrations, rotation, and larger security review surface. Good future direction; not Phase 1.
Sidecar/service per MCP: works but fragments configuration and creates duplication; a central broker avoids N copies.
8) Backward compatibility
Feature is opt-in; existing `backendAuth` methods are unchanged.
9) Rollout plan (small PRs)
PR-1: Docs + interface + implementation of `backendAuth.http`, unit tests, and docs page “Per-user OAuth (credential broker pattern)”.
PR-2 (optional): Admin read-only API/UI to display linked services by proxying broker metadata.
PR-3+: Consider built-in token exchange if maintainers prefer.
10) Acceptance criteria
Given a route with `backendAuth.http`, when a user calls a SaaS MCP target:
• If linked: requests include the user’s token; calls succeed.
• If not linked: client receives a propagated challenge (401/WWW-Authenticate or structured error with auth_url).
• Cache reduces broker calls on repeated requests for the same user/service.
No token material is logged; metrics exported.
11) Open questions for maintainers
Support multiple response shapes (e.g., json.token vs json.access_token) or keep one canonical field?
Prefer pure HTTP 401 pass-through or a typed error object exposed to MCP clients (or both, depending on transport)?
Templating syntax and available JWT claim paths — any preferences?
Is a minimal read-only “Linked Accounts” page (proxying broker data) desirable in Phase 1?
