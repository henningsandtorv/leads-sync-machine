import type { VercelRequest, VercelResponse } from "@vercel/node";
import "dotenv/config";

const APIFY_DATASET_URL_SYSTEK = process.env.APIFY_DATASET_URL_SYSTEK ?? "";
const APIFY_DATASET_URL_ILDER = process.env.APIFY_DATASET_URL_ILDER ?? "";
const INGEST_BASE_URL =
  process.env.INGEST_URL ?? process.env.VERCEL_URL
    ? `https://${process.env.VERCEL_URL}/ingest`
    : "http://localhost:3000/ingest";

async function postJob(job: any, source: "systek" | "ilder") {
  const endpoint = `${INGEST_BASE_URL}/apify-job-${source}`;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url: job.url,
      title: job.title,
      description: job.description,
      company: job.company,
      contactPersons: job.contactPersons ?? [],
      email: job.email,
      applicationUrl: job.applicationUrl,
      location: job.location,
      employmentType: job.employmentType,
      salary: job.salary,
      publicationDate: job.publicationDate,
      expirationDate: job.expirationDate,
      finnkode: job.finnkode,
      companyLogoUrl: job.companyLogoUrl,
      domain: job.domain,
      sector: job.sector,
      industries: job.industries,
      positionFunctions: job.positionFunctions,
      language: job.language,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Ingest failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function processDataset(datasetUrl: string, source: "systek" | "ilder") {
  const dataRes = await fetch(datasetUrl);
  if (!dataRes.ok) {
    throw new Error(`Failed to fetch dataset (${dataRes.status})`);
  }
  const items = await dataRes.json();
  if (!Array.isArray(items)) throw new Error("Dataset payload is not an array");

  let ok = 0;
  let failed = 0;
  for (const [i, item] of items.entries()) {
    try {
      await postJob(item, source);
      ok++;
    } catch (err: any) {
      failed++;
      console.error(
        `[${source.toUpperCase()}] Item ${i} failed:`,
        err?.message
      );
    }
  }
  return { ok, failed, total: items.length };
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  // Optional: Add authentication to prevent unauthorized access
  const authHeader = req.headers.authorization;
  if (
    process.env.CRON_SECRET &&
    authHeader !== `Bearer ${process.env.CRON_SECRET}`
  ) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const results: {
    systek?: { ok: number; failed: number; total: number };
    ilder?: { ok: number; failed: number; total: number };
  } = {};

  try {
    // Process SYSTEK dataset
    if (APIFY_DATASET_URL_SYSTEK) {
      try {
        results.systek = await processDataset(
          APIFY_DATASET_URL_SYSTEK,
          "systek"
        );
      } catch (err: any) {
        console.error("[SYSTEK] Fatal error:", err?.message);
        results.systek = { ok: 0, failed: 0, total: 0 };
      }
    }

    // Process ILDER dataset
    if (APIFY_DATASET_URL_ILDER) {
      try {
        results.ilder = await processDataset(APIFY_DATASET_URL_ILDER, "ilder");
      } catch (err: any) {
        console.error("[ILDER] Fatal error:", err?.message);
        results.ilder = { ok: 0, failed: 0, total: 0 };
      }
    }

    const totalOk = (results.systek?.ok ?? 0) + (results.ilder?.ok ?? 0);
    const totalFailed =
      (results.systek?.failed ?? 0) + (results.ilder?.failed ?? 0);
    const totalProcessed =
      (results.systek?.total ?? 0) + (results.ilder?.total ?? 0);

    return res.status(200).json({
      success: true,
      results,
      summary: {
        totalOk,
        totalFailed,
        totalProcessed,
      },
    });
  } catch (error: any) {
    console.error("Cron job failed:", error);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}
