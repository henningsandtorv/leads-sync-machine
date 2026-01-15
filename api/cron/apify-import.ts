import type { VercelRequest, VercelResponse } from "@vercel/node";
import "dotenv/config";
import { handleApifyJob } from "../../src/routes/ingest";

const APIFY_DATASET_URL_SYSTEK = process.env.APIFY_DATASET_URL_SYSTEK ?? "";
const APIFY_DATASET_URL_ILDER = process.env.APIFY_DATASET_URL_ILDER ?? "";

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
      // Create mock request/reply objects for the handler
      const mockRequest = {
        body: {
          url: item.url,
          title: item.title,
          description: item.description,
          company: item.company,
          contactPersons: item.contactPersons ?? [],
          email: item.email,
          applicationUrl: item.applicationUrl,
          location: item.location,
          employmentType: item.employmentType,
          salary: item.salary,
          publicationDate: item.publicationDate,
          expirationDate: item.expirationDate,
          finnkode: item.finnkode,
          companyLogoUrl: item.companyLogoUrl,
          domain: item.domain,
          sector: item.sector,
          industries: item.industries,
          positionFunctions: item.positionFunctions,
          language: item.language,
        },
      };

      let handlerError: Error | null = null;
      let handlerResponse: any = null;
      let statusCode = 200;

      const mockReply = {
        status: (code: number) => {
          statusCode = code;
          return {
            send: (data: any) => {
              if (code >= 400) {
                handlerError = new Error(
                  `Handler returned ${code}: ${JSON.stringify(data)}`
                );
              } else {
                handlerResponse = data;
              }
              return mockReply;
            },
          };
        },
        send: (data: any) => {
          handlerResponse = data;
          return mockReply;
        },
      };

      await handleApifyJob(mockRequest as any, mockReply as any, source);

      // Check if handler returned an error status
      if (statusCode >= 400 || handlerError) {
        throw (
          handlerError || new Error(`Handler returned status ${statusCode}`)
        );
      }

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
  // Require API secret authentication
  const CRON_SECRET = process.env.CRON_SECRET;

  if (!CRON_SECRET) {
    console.error("CRON_SECRET environment variable is not set");
    return res.status(500).json({
      error: "Internal server error",
    });
  }

  // Authentication: Accept Vercel Cron header OR Authorization Bearer token

  const authHeader = req.headers.authorization;
  const providedSecret = authHeader?.replace(/^Bearer\s+/i, "");

  if (providedSecret !== CRON_SECRET) {
    console.warn("Unauthorized cron job attempt", {
      ip: req.headers["x-forwarded-for"] || req.headers["x-real-ip"],
    });
    return res.status(401).json({
      error: "Unauthorized",
    });
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
      error: "Internal server error",
    });
  }
}
