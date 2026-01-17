/**
 * Clay.com webhook client for pushing job posts with decision makers
 */

import { z } from "zod";

// ============================================================
// Incoming Clay Enrichment Payload Schema (FROM Clay)
// ============================================================

const ClayEnrichedPersonSchema = z.object({
  full_name: z.string().min(1),
  title: z.string().nullable().optional(),
  email: z.string().nullable().optional(),
  phone: z.string().nullable().optional(),
  linkedin_url: z.string().nullable().optional(),
});

const ClayEnrichedCompanySchema = z.object({
  // Identifiers
  name: z.string().nullable().optional(),
  domain: z.string().nullable().optional(),
  orgnr: z.string().nullable().optional(),
  // Enrichable fields
  proff_url: z.string().nullable().optional(),
  industry: z.string().nullable().optional(),
  company_size: z.string().nullable().optional(),
  location: z.string().nullable().optional(),
  sector: z.string().nullable().optional(),
  profit_before_tax: z.string().nullable().optional(),
  turnover: z.string().nullable().optional(),
});

export const ClayEnrichmentPayloadSchema = z.object({
  // Required anchor to find the company via job_posts
  finn_id: z.string().min(1),
  // Enriched company data
  company: ClayEnrichedCompanySchema.optional(),
  // New decision makers found by Clay
  decision_makers: z.array(ClayEnrichedPersonSchema).default([]),
  // Enriched contact person data
  contact_persons: z.array(ClayEnrichedPersonSchema).default([]),
});

export type ClayEnrichmentPayload = z.infer<typeof ClayEnrichmentPayloadSchema>;
export type ClayEnrichedPerson = z.infer<typeof ClayEnrichedPersonSchema>;

// ============================================================
// Outgoing Clay Payload Types (TO Clay)
// ============================================================

export type ClayPerson = {
  full_name: string;
  title: string | null;
  email: string | null;
  phone: string | null;
  linkedin_url: string | null;
};

/** @deprecated Use ClayPerson instead */
export type ClayDecisionMaker = ClayPerson;

export type ClayJobPostPayload = {
  job_post: {
    finn_id: string;
    finn_url: string;
    title: string | null;
    description: string | null;
    location: string | null;
    employment_type: string | null;
    salary: string | null;
    publication_date: string | null;
    expiration_date: string | null;
    application_url: string | null;
    sector: string | null;
    industries: string[] | null;
    source: string | null;
  };
  company: {
    name: string | null;
    domain: string | null;
    clean_domain: string | null;
    orgnr: string | null;
    proff_url: string | null;
    industry: string | null;
    company_size: string | null;
    location: string | null;
    sector: string | null;
    profit_before_tax: string | null;
    turnover: string | null;
  };
  decision_makers: ClayPerson[];
  contact_persons: ClayPerson[];
  decision_makers_formatted: string | null;
  contact_persons_formatted: string | null;
};

const CLAY_CELL_MAX_BYTES = 6000; // Clay limit is 8KB, use 6KB for safety

/**
 * Truncate description to fit within Clay cell limits while preserving key info.
 * Tries to break at paragraph or sentence boundaries.
 */
export function truncateDescription(
  description: string | null
): string | null {
  if (!description) return null;

  const bytes = Buffer.byteLength(description, "utf8");
  if (bytes <= CLAY_CELL_MAX_BYTES) return description;

  // Find a good cutoff point within the limit
  let cutoff = CLAY_CELL_MAX_BYTES;

  // Work backwards to find a safe UTF-8 boundary
  while (cutoff > 0 && Buffer.byteLength(description.slice(0, cutoff), "utf8") > CLAY_CELL_MAX_BYTES) {
    cutoff--;
  }

  let truncated = description.slice(0, cutoff);

  // Try to break at paragraph boundary
  const lastParagraph = truncated.lastIndexOf("\n\n");
  if (lastParagraph > cutoff * 0.7) {
    truncated = truncated.slice(0, lastParagraph);
  } else {
    // Try to break at sentence boundary
    const lastSentence = Math.max(
      truncated.lastIndexOf(". "),
      truncated.lastIndexOf(".\n"),
      truncated.lastIndexOf("? "),
      truncated.lastIndexOf("! ")
    );
    if (lastSentence > cutoff * 0.7) {
      truncated = truncated.slice(0, lastSentence + 1);
    }
  }

  return truncated.trim() + "\n\n[...]";
}

/**
 * Format a single person as a compact single line
 * Format: Name, Title, email, phone, linkedin
 */
function formatPersonLine(person: ClayPerson): string {
  const parts = [person.full_name];
  if (person.title) parts.push(person.title);
  if (person.email) parts.push(person.email);
  if (person.phone) parts.push(person.phone);
  if (person.linkedin_url) parts.push(person.linkedin_url);
  return parts.join(", ");
}

/**
 * Format a list of persons into a compact text field
 * Each person on a new line with: Name, Title, email, phone, linkedin
 */
export function formatPersonsCompact(persons: ClayPerson[]): string | null {
  if (persons.length === 0) return null;
  return persons.map(formatPersonLine).join("\n");
}

const CLAY_WEBHOOK_URL = process.env.CLAY_WEBHOOK_URL;

/**
 * Check if Clay webhook is configured
 */
export function isClayWebhookEnabled(): boolean {
  return !!CLAY_WEBHOOK_URL;
}

/**
 * Send job post with decision makers to Clay webhook
 * Returns true on success, false on failure (logs errors but doesn't throw)
 */
export async function sendToClayWebhook(
  payload: ClayJobPostPayload
): Promise<boolean> {
  if (!CLAY_WEBHOOK_URL) {
    console.log("[Clay] Webhook URL not configured, skipping");
    return false;
  }

  try {
    const response = await fetch(CLAY_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error(
        `[Clay] Webhook failed: ${response.status} ${response.statusText}`
      );
      return false;
    }

    console.log(
      `[Clay] Successfully sent job post ${payload.job_post.finn_id} to Clay`
    );
    return true;
  } catch (error) {
    console.error("[Clay] Webhook error:", error);
    return false;
  }
}

/**
 * Send multiple job posts to Clay (for batch sync)
 * Includes delay between requests to avoid rate limiting
 */
export async function sendBatchToClayWebhook(
  payloads: ClayJobPostPayload[],
  delayMs: number = 100
): Promise<{ success: number; failed: number }> {
  let success = 0;
  let failed = 0;

  for (const payload of payloads) {
    const result = await sendToClayWebhook(payload);
    if (result) {
      success++;
    } else {
      failed++;
    }

    // Add delay between requests to avoid rate limiting
    if (delayMs > 0 && payloads.indexOf(payload) < payloads.length - 1) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }

  return { success, failed };
}
