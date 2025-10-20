import { Policies, RouteLike } from '../types/policies';

function mergePolicies(acc: Policies, next?: Policies | Record<string, unknown>): Policies {
  if (!next || typeof next !== 'object') return acc;
  return { ...acc, ...(next as Policies) };
}

/** Extract a Policies object from both file-mode and xDS/PolicyAttachment shapes. */
export function extractPolicies(route: RouteLike | undefined | null): Policies | undefined {
  if (!route) return undefined;

  if (route.policies && typeof route.policies === 'object') {
    return route.policies as Policies;
  }

  if (Array.isArray(route.policyAttachments) && route.policyAttachments.length > 0) {
    return route.policyAttachments.reduce<Policies>((acc, att) => {
      const src = (att.effective as Policies) ?? (att.spec as Policies);
      return mergePolicies(acc, src);
    }, {});
  }

  return undefined;
}
