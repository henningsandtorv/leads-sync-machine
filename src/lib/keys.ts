import {
  canonicalizeLinkedInUrl,
  nameSlug,
  normalizeDomainHost,
  normalizeEmail,
  normalizePhone,
  normalizeNameForKey,
  normalizeOrgnr,
} from "./normalize";

export function buildCompanyKey(params: {
  orgnr?: string | null;
  clean_domain?: string | null;
  domain_host?: string | null;
  name?: string | null;
}) {
  const { orgnr, clean_domain, domain_host, name } = params;

  // Priority 1: Prefer orgnr if available (normalized: no spaces)
  const normalizedOrgnr = normalizeOrgnr(orgnr);
  if (normalizedOrgnr) {
    return normalizedOrgnr; // Just the number, no prefix
  }

  // Priority 2: Use clean_domain if available (preferred over domain)
  if (clean_domain) {
    const normalizedCleanDomain = normalizeDomainHost(clean_domain);
    if (normalizedCleanDomain && normalizedCleanDomain !== "finn.no") {
      return normalizedCleanDomain; // Just the domain, no prefix
    }
  }

  // Priority 3: Try domain_host, but skip if it's finn.no (scraper default)
  const normalizedDomain = normalizeDomainHost(domain_host);
  if (normalizedDomain && normalizedDomain !== "finn.no") {
    return normalizedDomain; // Just the domain, no prefix
  }

  // Priority 4: Fall back to normalized company name
  const slug = nameSlug(name);
  if (slug) {
    return slug;
  }

  throw new Error("Unable to derive company_key");
}

export function buildPersonKey(params: {
  linkedin_url?: string | null;
  email?: string | null;
  phone?: string | null;
  company_key?: string | null;
  full_name?: string | null;
  company_name?: string | null; // Add company_name for name-based keys
}) {
  // Prefer unique identifiers (no prefixes)
  const linkedin = canonicalizeLinkedInUrl(params.linkedin_url);
  if (linkedin) return linkedin; // Just the URL, no prefix

  const email = normalizeEmail(params.email);
  if (email) return email; // Just the email, no prefix

  const phone = normalizePhone(params.phone);
  if (phone) return phone; // Just the phone, no prefix

  // Fallback: use company_name + person_name format
  // Format: "{normalized_company_name}_{normalized_person_name}"
  const personName = normalizeNameForKey(params.full_name);
  if (personName) {
    // Use company_name if provided, otherwise fall back to company_key
    // If company_key is a domain, we need to get the company name from somewhere
    // For now, use company_name if available, otherwise use company_key as-is
    const companyName = params.company_name
      ? normalizeNameForKey(params.company_name)
      : params.company_key; // company_key is already normalized (domain or slug)

    if (companyName) {
      return `${companyName}_${personName}`;
    }
  }

  throw new Error("Unable to derive person_key");
}
