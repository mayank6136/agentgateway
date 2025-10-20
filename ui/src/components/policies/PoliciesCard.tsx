import React from 'react';
import { Policies } from '../../types/policies';
import { PolicyPill } from './PolicyPill';

function defined(v: unknown) {
  if (v === null || v === undefined) return false;
  if (typeof v === 'object') return Object.keys(v as object).length > 0;
  return true;
}

export const PoliciesCard: React.FC<{ policies?: Policies }> = ({ policies }) => {
  if (!policies || !defined(policies)) {
    return (
      <section aria-label="Policies">
        <h3 style={{ margin: '1rem 0 .5rem' }}>Policies</h3>
        <div style={{ fontStyle: 'italic', color: '#6E7781' }}>No policies attached.</div>
      </section>
    );
  }

  const items: Array<[string, string | undefined]> = [];

  if (defined(policies.cors)) items.push(['CORS', undefined]);
  if (defined(policies.jwtAuth)) items.push(['JWT Auth', undefined]);
  if (defined(policies.mcpAuthentication)) items.push(['MCP AuthN', undefined]);
  if (defined(policies.mcpAuthorization)) items.push(['MCP AuthZ', undefined]);
  if (defined(policies.backendAuth)) items.push(['Backend Auth', undefined]);
  if (defined(policies.backendTLS)) items.push(['Backend TLS', undefined]);

  if (defined(policies.rateLimit)) items.push(['Rate limit', undefined]);
  if (defined(policies.retries)) items.push(['Retries', undefined]);
  if (defined(policies.timeout)) items.push(['Timeouts', undefined]);
  if (defined(policies.mirror)) items.push(['Mirroring', undefined]);

  if (defined(policies.headers)) items.push(['Headers', undefined]);
  if (defined(policies.redirect)) items.push(['Redirect', undefined]);
  if (defined(policies.rewrite)) items.push(['Rewrite', undefined]);
  if (defined(policies.directResponse)) items.push(['Direct response', undefined]);

  if (defined(policies.ai)) items.push(['AI', undefined]);
  if (defined(policies.a2a)) items.push(['A2A', undefined]);

  const json = JSON.stringify(policies, null, 2);

  return (
    <section aria-label="Policies">
      <h3 style={{ margin: '1rem 0 .5rem' }}>Policies</h3>
      <div>
        {items.length === 0 ? (
          <div style={{ fontStyle: 'italic', color: '#6E7781' }}>No policies attached.</div>
        ) : (
          items.map(([label, title]) => <PolicyPill key={label} label={label} title={title} />)
        )}
      </div>

      <details style={{ marginTop: '.75rem' }}>
        <summary style={{ cursor: 'pointer' }}>View JSON</summary>
        <pre
          style={{
            marginTop: '.5rem',
            background: '#F6F8FA',
            border: '1px solid #D0D7DE',
            borderRadius: 6,
            padding: 12,
            overflowX: 'auto',
            fontSize: 12,
          }}
        >
{json}
        </pre>
      </details>
    </section>
  );
};
