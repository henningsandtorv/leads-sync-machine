import { URL } from "node:url";

const stripTrailingSlash = (value: string) => value.replace(/\/+$/, "");

export function normalizeDomainHost(input?: string | null): string | null {
  if (!input) return null;
  const trimmed = input.trim();
  if (!trimmed) return null;

  let hostname: string;
  try {
    const url = new URL(
      trimmed.includes("://") ? trimmed : `https://${trimmed}`
    );
    hostname = url.hostname.toLowerCase();
  } catch {
    // Fallback: basic normalization
    const withoutScheme = trimmed.replace(/^[a-z]+:\/\//i, "");
    hostname = withoutScheme.split("/")[0].toLowerCase();
  }

  // Strip common prefixes like www., www2., www3., etc.
  // This ensures "www.wpg.no" becomes "wpg.no"
  hostname = hostname.replace(/^www\d*\./i, "");

  return hostname || null;
}

export function extractFinnIdFromUrl(url?: string | null): string | null {
  if (!url) return null;
  const match = url.match(/finn\.no\/job\/ad\/(\d+)/i);
  return match ? match[1] : null;
}

export function canonicalizeLinkedInUrl(url?: string | null): string | null {
  if (!url) return null;
  const trimmed = url.trim();
  if (!trimmed) return null;
  try {
    const parsed = new URL(trimmed.toLowerCase());
    if (!parsed.hostname.includes("linkedin.")) return null;

    // Normalize country subdomains (no.linkedin.com, uk.linkedin.com, etc.) to www.linkedin.com
    // This ensures https://no.linkedin.com/in/abraham-foss-0865b32 and
    // https://www.linkedin.com/in/abraham-foss-0865b32 are treated as the same
    if (parsed.hostname.includes("linkedin.com")) {
      parsed.hostname = "www.linkedin.com";
    }

    parsed.search = "";
    parsed.hash = "";
    return stripTrailingSlash(parsed.toString());
  } catch {
    return null;
  }
}

export function normalizeEmail(email?: string | null): string | null {
  if (!email) return null;
  const trimmed = email.trim().toLowerCase();
  return trimmed || null;
}

export function normalizePhone(phone?: string | null): string | null {
  if (!phone) return null;
  const trimmed = phone.trim();
  const digits = trimmed.replace(/\D+/g, "");
  if (!digits) return null;

  // Handle Norwegian numbers with country code
  // Only strip +47/0047 prefix if the original input clearly indicates a country code
  // This avoids ambiguity with numbers that legitimately start with "47"
  const hasCountryCodeIndicator =
    trimmed.startsWith("+47") ||
    trimmed.startsWith("0047") ||
    trimmed.startsWith("47 ") || // Space after 47 suggests country code
    trimmed.startsWith("47-"); // Dash after 47 suggests country code

  if (hasCountryCodeIndicator && digits.startsWith("47") && digits.length === 10) {
    // Norwegian number with explicit country code, remove it
    return digits.substring(2);
  }

  // Handle 00 prefix (international dialing)
  if (digits.startsWith("0047") && digits.length === 12) {
    return digits.substring(4);
  }

  // If it's already 8 digits, assume it's a valid Norwegian number
  // Norwegian mobile numbers start with 4, 9, or 8
  // Norwegian landlines start with 2, 3, 5, 6, or 7
  return digits;
}

/**
 * Normalizes organization number (orgnr) by removing spaces and trimming
 */
export function normalizeOrgnr(orgnr?: string | null): string | null {
  if (!orgnr) return null;
  // Remove all spaces and trim
  const normalized = orgnr.replace(/\s+/g, "").trim();
  return normalized || null;
}

export function nameSlug(name?: string | null): string | null {
  if (!name) return null;
  const slug = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .slice(0, 80);
  return slug || null;
}

/**
 * Normalizes a name for use in person keys - lowercase, trimmed, spaces preserved
 */
export function normalizeNameForKey(name?: string | null): string | null {
  if (!name) return null;
  return name
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ") // Normalize multiple spaces to single space
    .slice(0, 100);
}

/**
 * Normalizes a name for comparison/deduplication - handles different capitalizations
 */
export function normalizeNameForComparison(
  name?: string | null
): string | null {
  if (!name) return null;
  return name
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ") // Normalize multiple spaces to single space
    .replace(/[^\w\s]/g, "") // Remove special characters for comparison
    .slice(0, 100);
}

/**
 * Normalizes company name for matching by stripping common legal suffixes.
 * Handles Norwegian and international company types:
 * - AS, ASA, A/S (Norwegian)
 * - Ltd, Limited, Inc, Corp, Corporation, LLC, GmbH, etc.
 *
 * Example: "AKVA Group ASA" -> "akvagroup"
 *          "AKVA group" -> "akvagroup"
 */
export function normalizeCompanyNameForMatching(
  name?: string | null
): string | null {
  if (!name) return null;

  let normalized = name.trim().toLowerCase();

  // Remove common legal suffixes (order matters - longer ones first)
  const suffixes = [
    // Norwegian
    "\\s+asa$",
    "\\s+a/s$",
    "\\s+as$",
    "\\s+ans$",
    "\\s+da$",
    "\\s+ba$",
    "\\s+sa$",
    "\\s+nuf$",
    "\\s+ks$",
    // International
    "\\s+corporation$",
    "\\s+incorporated$",
    "\\s+limited$",
    "\\s+company$",
    "\\s+corp\\.?$",
    "\\s+inc\\.?$",
    "\\s+ltd\\.?$",
    "\\s+llc$",
    "\\s+llp$",
    "\\s+gmbh$",
    "\\s+ag$",
    "\\s+bv$",
    "\\s+nv$",
    "\\s+plc$",
    "\\s+co\\.?$",
    // Generic
    "\\s+group$",
    "\\s+holding$",
    "\\s+holdings$",
  ];

  for (const suffix of suffixes) {
    normalized = normalized.replace(new RegExp(suffix, "i"), "");
  }

  // Remove all non-alphanumeric characters and collapse to single string
  normalized = normalized.replace(/[^a-z0-9æøåäöü]+/g, "");

  return normalized || null;
}

/**
 * Normalizes date strings to ISO 8601 format for PostgreSQL.
 * Handles Norwegian date formats (DD.MM.YYYY) and non-date values like "Snarest".
 */
export function normalizeDate(dateStr?: string | null): string | null {
  if (!dateStr) return null;
  const trimmed = dateStr.trim();
  if (!trimmed) return null;

  // Handle non-date values (Norwegian: "Snarest" = "as soon as possible")
  const lower = trimmed.toLowerCase();
  if (lower === "snarest" || lower === "asap" || lower === "immediately") {
    return null;
  }

  // Try ISO 8601 format first (already valid)
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    try {
      const d = new Date(trimmed);
      if (!isNaN(d.getTime())) {
        return d.toISOString();
      }
    } catch {
      // Fall through to other formats
    }
  }

  // Try Norwegian format: DD.MM.YYYY or D.M.YYYY
  const norwegianMatch = trimmed.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (norwegianMatch) {
    const [, day, month, year] = norwegianMatch;
    try {
      const d = new Date(
        `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`
      );
      if (!isNaN(d.getTime())) {
        return d.toISOString();
      }
    } catch {
      // Fall through
    }
  }

  // Try other common formats
  try {
    const d = new Date(trimmed);
    if (!isNaN(d.getTime())) {
      return d.toISOString();
    }
  } catch {
    // Invalid date, return null
  }

  return null;
}

/**
 * Classifies a person's role based on their title
 * Returns one of: 'decision_maker', 'recruiter', 'contact_person', 'other'
 */
export function classifyPersonRole(
  title: string | null | undefined
): "decision_maker" | "recruiter" | "contact_person" | "other" {
  if (!title) {
    return "contact_person"; // Default if no title
  }

  const normalizedTitle = title.toLowerCase().trim();

  // Decision maker keywords (executives, managers, directors, owners)
  // Using word boundary patterns where needed to avoid false positives
  const decisionMakerKeywords = [
    "ceo",
    "chief executive",
    "chief executive officer",
    "cto",
    "chief technology",
    "chief technical",
    "cfo",
    "chief financial",
    "coo",
    "chief operating",
    "president",
    "founder",
    "owner",
    "proprietor",
    "director",
    "managing director",
    "general manager",
    "vp",
    "vice president",
    "vice-president",
    "head of", // "head of" is specific enough
    "team lead",
    "tech lead",
    "engineering lead",
    "project lead",
    "manager",
    "senior manager",
    "partner",
    "principal",
    "executive",
    "it manager",
    "it director",
    "it chef",
    // Norwegian decision maker titles
    "daglig leder",
    "sjef", // Norwegian for "boss/chief" - usually specific enough
    "direktør",
    "administrerende direktør",
    "adm. direktør",
    "styreleder",
    "eier",
    "gründer",
    "leder", // Note: this is common in Norwegian titles like "avdelingsleder"
    "avdelingsleder",
    "prosjektleder",
    "teamleder",
    "områdeleder",
    "regionleder",
    "salgsdirektør",
    "markedsdirektør",
    "teknisk direktør",
    "finansdirektør",
    "operasjonsdirektør",
    "produksjonsleder",
    "butikksjef",
    "kontorsjef",
    "avdelingssjef",
  ];

  // Recruiter keywords
  const recruiterKeywords = [
    "recruiter",
    "recruitment",
    "talent acquisition",
    "talent",
    "hiring",
    "hr",
    "human resources",
    "people",
    "people operations",
    "people ops",
    "staffing",
    "sourcing",
    "recruiting",
    "talent manager",
    "talent partner",
    // Norwegian recruiter titles
    "rekrutteringsansvarlig",
    "rekrutteringskonsulent",
    "rekrutterer",
    "rekruttering",
    "talentansvarlig",
    "talentkonsulent",
    "ansettelsesansvarlig",
    "ansettelsesleder",
    "personalsjef",
    "personalspesialist",
    "hr-sjef",
    "hr-konsulent",
    "hr-spesialist",
    "personalkonsulent",
    "ansettelseskonsulent",
  ];

  // Check for decision maker first (higher priority)
  for (const keyword of decisionMakerKeywords) {
    if (normalizedTitle.includes(keyword)) {
      return "decision_maker";
    }
  }

  // Check for recruiter
  for (const keyword of recruiterKeywords) {
    if (normalizedTitle.includes(keyword)) {
      return "recruiter";
    }
  }

  // Default to contact_person if no match
  return "contact_person";
}

/**
 * Validates that a name has at least 2 words (first + last name).
 * Single-word names like "Wiggen" are not valid identifiable names.
 */
export function isValidPersonName(name?: string | null): boolean {
  if (!name) return false;
  const trimmed = name.trim();
  const words = trimmed.split(/\s+/).filter((w) => w.length > 0);
  return words.length >= 2;
}
