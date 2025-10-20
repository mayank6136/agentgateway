/* Minimal, forward-compatible policy types used by the read-only UI. */
export type CorsPolicy = {
  allowOrigins?: string[];
  allowHeaders?: string[];
  allowMethods?: string[];
  exposeHeaders?: string[];
  allowCredentials?: boolean;
};

export type JwtAuthPolicy = {
  mode?: 'strict' | 'optional' | 'permissive';
  issuer?: string;
  audiences?: string[];
  jwks?: { url?: string; file?: string };
};

export type BackendAuthPolicy =
  | { passthrough?: boolean }
  | { key?: string | { file?: string } }
  | { gcp?: Record<string, unknown> }
  | { aws?: Record<string, unknown> };

export type BackendTLSPolicy = {
  sni?: string;
  caCert?: { file?: string };
  clientCert?: { certFile?: string; keyFile?: string };
  insecureSkipVerify?: boolean;
};

export type LocalRateLimit = {
  requestsPerUnit?: number;
  unit?: 'SECOND' | 'MINUTE' | 'HOUR' | string;
  burst?: number;
};

export type RemoteRateLimit = {
  provider?: string;
  config?: Record<string, unknown>;
};

export type RateLimitPolicy = {
  local?: LocalRateLimit;
  remote?: RemoteRateLimit;
};

export type TimeoutPolicy = {
  requestMs?: number;
  idleMs?: number;
  perTryMs?: number;
};

export type RetryPolicy = {
  attempts?: number;
  perTryTimeoutMs?: number;
  retryOn?: string[];
};

export type MirrorPolicy = {
  percentage?: number;
  backend?: string;
};

export type HeaderOps = {
  add?: Record<string, string>;
  set?: Record<string, string>;
  remove?: string[];
};
export type HeaderManipulationPolicy = {
  request?: HeaderOps;
  response?: HeaderOps;
};

export type RedirectPolicy = {
  scheme?: string;
  host?: string;
  path?: string;
  statusCode?: number;
};

export type RewritePolicy = {
  host?: string;
  pathPrefix?: string;
  pathRegex?: string;
  method?: string;
};

export type DirectResponsePolicy = {
  status?: number;
  body?: string;
};

export type ExtAuthzPolicy = {
  url?: string;
  headers?: Record<string, string>;
};

export type MCAAuthPolicy = Record<string, unknown>;        // MCP authN
export type MCAuthorizationPolicy = Record<string, unknown>; // MCP authZ
export type AIBackendPolicy = Record<string, unknown>;       // AI-specific knobs
export type A2APolicy = Record<string, unknown>;             // A2A-specific knobs

export type Policies = {
  cors?: CorsPolicy;
  jwtAuth?: JwtAuthPolicy;
  backendAuth?: BackendAuthPolicy;
  backendTLS?: BackendTLSPolicy;
  rateLimit?: RateLimitPolicy;
  timeout?: TimeoutPolicy;
  retries?: RetryPolicy;
  mirror?: MirrorPolicy;
  headers?: HeaderManipulationPolicy;
  redirect?: RedirectPolicy;
  rewrite?: RewritePolicy;
  directResponse?: DirectResponsePolicy;
  extAuthz?: ExtAuthzPolicy;
  mcpAuthentication?: MCAAuthPolicy;
  mcpAuthorization?: MCAuthorizationPolicy;
  ai?: AIBackendPolicy;
  a2a?: A2APolicy;
  // Forward compatibility:
  [k: string]: unknown;
};

export type RouteLike = {
  name?: string;
  policies?: Policies; // file-mode
  policyAttachments?: Array<{
    kind: string;
    name?: string;
    namespace?: string;
    spec?: Policies | Record<string, unknown>;
    effective?: Policies | Record<string, unknown>;
  }>;
  [k: string]: unknown;
};
