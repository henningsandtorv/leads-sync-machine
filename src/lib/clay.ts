/**
 * Clay.com webhook client for pushing job posts with decision makers
 */

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
};

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
