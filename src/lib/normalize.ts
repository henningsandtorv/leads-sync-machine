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
  const digits = phone.replace(/\D+/g, "");
  if (!digits) return null;

  // Handle Norwegian numbers: remove country code if present
  // "+47 95 46 76 01" -> "4795467601" -> "95467601" (if 8 digits after removing 47)
  // "95 46 76 01" -> "95467601"
  if (digits.startsWith("47") && digits.length === 10) {
    // Norwegian number with country code, remove it
    return digits.substring(2);
  }

  // If it's 8 digits and starts with 4, 9, or 8, it's likely a Norwegian mobile number
  // Keep as-is
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
