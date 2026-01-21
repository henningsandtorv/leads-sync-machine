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
  company_domain?: string | null;
  company_key?: string | null;
  full_name?: string | null;
  company_name?: string | null;
}) {
  // Priority 1: LinkedIn URL (unique identifier)
  const linkedin = canonicalizeLinkedInUrl(params.linkedin_url);
  if (linkedin) return linkedin;

  // Priority 2: Email (unique identifier)
  const email = normalizeEmail(params.email);
  if (email) return email;

  // Priority 3: Phone (unique identifier)
  const phone = normalizePhone(params.phone);
  if (phone) return phone;

  // Fallback: use domain or company_name + person_name
  const personName = normalizeNameForKey(params.full_name);
  if (personName) {
    // Priority 4: Domain + name (preferred - domain is stable)
    const domain = normalizeDomainHost(params.company_domain);
    if (domain && domain !== "finn.no") {
      return `${domain}_${personName}`;
    }

    // Priority 5: Company name + person name (last resort)
    const companyName = params.company_name
      ? normalizeNameForKey(params.company_name)
      : params.company_key;

    if (companyName) {
      return `${companyName}_${personName}`;
    }
  }

  throw new Error("Unable to derive person_key");
}
