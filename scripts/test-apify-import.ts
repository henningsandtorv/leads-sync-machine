import "dotenv/config";

// Config
const APIFY_DATASET_URL_SYSTEK = process.env.APIFY_DATASET_URL_SYSTEK ?? "";
const APIFY_DATASET_URL_ILDER = process.env.APIFY_DATASET_URL_ILDER ?? "";
const INGEST_BASE_URL =
  process.env.INGEST_URL ?? "http://localhost:5432/ingest";

// Simple helper
async function postJob(job: any, source: "systek" | "ilder") {
  const payload = {
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
  };

  const endpoint = `${INGEST_BASE_URL}/apify-job-${source}`;
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Ingest failed (${res.status}): ${text}`);
    }
    return res.json();
  } catch (err: any) {
    // Provide more detailed error information
    if (err.code === "ECONNREFUSED" || err.message?.includes("ECONNREFUSED")) {
      throw new Error(
        `Connection refused - is the server running at ${endpoint}? Start it with: npm run dev`
      );
    }
    if (err.cause) {
      throw new Error(`Fetch failed: ${err.message} (${err.cause.message})`);
    }
    throw err;
  }
}

async function processDataset(datasetUrl: string, source: "systek" | "ilder") {
  console.log(
    `\n[${source.toUpperCase()}] Fetching Apify dataset from`,
    datasetUrl
  );
  const dataRes = await fetch(datasetUrl);
  if (!dataRes.ok) {
    const text = await dataRes.text();
    throw new Error(`Failed to fetch dataset (${dataRes.status}): ${text}`);
  }
  const items = await dataRes.json();
  if (!Array.isArray(items)) throw new Error("Dataset payload is not an array");

  console.log(
    `[${source.toUpperCase()}] Fetched ${items.length} items. Posting...`
  );
  let ok = 0;
  let failed = 0;
  for (const [i, item] of items.entries()) {
    try {
      await postJob(item, source);
      ok++;
      if (ok % 25 === 0)
        console.log(`[${source.toUpperCase()}] Posted ${ok}/${items.length}`);
    } catch (err: any) {
      failed++;
      console.error(
        `[${source.toUpperCase()}] Item ${i} failed:`,
        err?.message ?? err
      );
    }
  }
  console.log(
    `[${source.toUpperCase()}] Done. Success: ${ok}/${
      items.length
    }, Failed: ${failed}`
  );
  return { ok, failed, total: items.length };
}

async function main() {
  const results: {
    systek?: { ok: number; failed: number; total: number };
    ilder?: { ok: number; failed: number; total: number };
  } = {};

  // Process SYSTEK dataset
  if (APIFY_DATASET_URL_SYSTEK) {
    try {
      results.systek = await processDataset(APIFY_DATASET_URL_SYSTEK, "systek");
    } catch (err: any) {
      console.error("[SYSTEK] Fatal error:", err?.message ?? err);
    }
  } else {
    console.warn("[SYSTEK] APIFY_DATASET_URL_SYSTEK not set, skipping");
  }

  // Process ILDER dataset
  if (APIFY_DATASET_URL_ILDER) {
    try {
      results.ilder = await processDataset(APIFY_DATASET_URL_ILDER, "ilder");
    } catch (err: any) {
      console.error("[ILDER] Fatal error:", err?.message ?? err);
    }
  } else {
    console.warn("[ILDER] APIFY_DATASET_URL_ILDER not set, skipping");
  }

  // Summary
  console.log("\n=== Summary ===");
  if (results.systek) {
    console.log(
      `SYSTEK: ${results.systek.ok}/${results.systek.total} succeeded, ${results.systek.failed} failed`
    );
  }
  if (results.ilder) {
    console.log(
      `ILDER: ${results.ilder.ok}/${results.ilder.total} succeeded, ${results.ilder.failed} failed`
    );
  }
  const totalOk = (results.systek?.ok ?? 0) + (results.ilder?.ok ?? 0);
  const totalFailed =
    (results.systek?.failed ?? 0) + (results.ilder?.failed ?? 0);
  const totalProcessed =
    (results.systek?.total ?? 0) + (results.ilder?.total ?? 0);
  console.log(
    `Total: ${totalOk}/${totalProcessed} succeeded, ${totalFailed} failed`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
