import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { parse } from "csv-parse/sync";
import { createClient } from "@supabase/supabase-js";
import {
  canonicalizeLinkedInUrl,
  normalizeEmail,
  normalizePhone,
  normalizeNameForKey,
  normalizeNameForComparison,
  normalizeDomainHost,
  normalizeOrgnr,
} from "../src/lib/normalize";
import { buildPersonKey, buildCompanyKey } from "../src/lib/keys";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  {
    db: {
      schema: "leadgen",
    },
  }
);

const BASE = path.join(process.cwd(), "data", "import");
const CHUNK = 500;

type CsvCompany = {
  company_key: string;
  name?: string;
  domain?: string;
  clean_domain?: string;
  orgnr?: string;
  proff_url?: string;
  industry?: string;
  company_size?: string;
  location?: string;
  profit_before_tax?: string;
  turnover?: string;
};

type CsvPerson = {
  person_key: string;
  full_name: string;
  title?: string;
  email?: string;
  phone?: string;
  linkedin_url?: string;
  normalized_company_name?: string;
  normalized_company_domain?: string;
};

type CsvJob = {
  finn_url: string;
  company_key: string;
  company_name_raw?: string;
  job_title?: string;
  description?: string;
  application_url?: string;
  location?: string;
  employment_type?: string;
  publication_date?: string;
  expiration_date_raw?: string;
  company_logo_url?: string;
  scraped_at?: string;
  status?: string;
  // Optional contact fields for auto-linking to people
  contact_name?: string;
  contact_email?: string;
  contact_phone?: string;
  contact_role?: string;
  contact_linkedin?: string;
};

function readCsv<T>(file: string): T[] {
  const text = fs.readFileSync(file, "utf8");
  return parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  }) as T[];
}

/**
 * Read multiple CSV files matching a pattern and combine them
 */
function readMultipleCsv<T>(pattern: RegExp): T[] {
  const files = fs
    .readdirSync(BASE)
    .filter((file) => pattern.test(file))
    .sort(); // Sort for consistent ordering

  if (files.length === 0) {
    console.warn(`No files found matching pattern: ${pattern}`);
    return [];
  }

  console.log(
    `Reading ${files.length} file(s) matching pattern: ${files.join(", ")}`
  );

  const allRows: T[] = [];
  for (const file of files) {
    const filePath = path.join(BASE, file);
    const rows = readCsv<T>(filePath);
    console.log(`  - ${file}: ${rows.length} rows`);
    allRows.push(...rows);
  }

  return allRows;
}

function chunk<T>(arr: T[], size: number) {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function pick<T extends Record<string, any>, K extends keyof T>(
  obj: T,
  keys: readonly K[]
): Partial<Pick<T, K>> {
  return keys.reduce((acc, key) => {
    const val = obj[key];
    if (val !== undefined) {
      (acc as any)[key] = val === "" ? null : val;
    }
    return acc;
  }, {} as Partial<Pick<T, K>>);
}

function fail(ctx: string, error: any) {
  if (!error) return;
  console.error(`${ctx} failed:`, JSON.stringify(error, null, 2));
  throw error;
}

function emailDomain(email?: string) {
  if (!email) return null;
  const m = email.toLowerCase().match(/@([^@]+)$/);
  return m ? m[1] : null;
}

function normalizeDomain(s?: string | null) {
  if (!s) return null;
  return s
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "");
}

function normalizeName(s?: string | null) {
  if (!s) return null;
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

/**
 * Deduplicate companies by checking orgnr, clean_domain, domain, and normalized name
 * If any identifier matches, group them together
 */
function deduplicateCompanies(rows: CsvCompany[]): CsvCompany[] {
  // First, normalize all identifiers
  const normalizedRows = rows.map((row) => ({
    ...row,
    orgnr: normalizeOrgnr(row.orgnr) || undefined,
    clean_domain: normalizeDomainHost(row.clean_domain) || undefined,
    domain: normalizeDomainHost(row.domain) || undefined,
    name_normalized: normalizeName(row.name) || undefined, // Use normalizeName for comparison (slug format)
  }));

  // Group by checking all identifiers - if any match, group together
  const groups = new Map<string, CsvCompany[]>();
  const identifierToGroup = new Map<string, string>(); // identifier -> groupKey

  for (const row of normalizedRows) {
    // Find existing group by checking all identifiers
    let groupKey: string | undefined;

    // Check if any identifier already has a group
    if (row.orgnr) {
      groupKey = identifierToGroup.get(`orgnr:${row.orgnr}`);
    }
    if (!groupKey && row.clean_domain) {
      groupKey = identifierToGroup.get(`clean_domain:${row.clean_domain}`);
    }
    if (!groupKey && row.domain) {
      groupKey = identifierToGroup.get(`domain:${row.domain}`);
    }
    if (!groupKey && row.name_normalized) {
      groupKey = identifierToGroup.get(`name:${row.name_normalized}`);
    }

    // If no existing group, create a new one using the best identifier
    if (!groupKey) {
      if (row.orgnr) {
        groupKey = `orgnr:${row.orgnr}`;
      } else if (row.clean_domain) {
        groupKey = `clean_domain:${row.clean_domain}`;
      } else if (row.domain) {
        groupKey = `domain:${row.domain}`;
      } else if (row.name_normalized) {
        groupKey = `name:${row.name_normalized}`;
      } else {
        // Fallback to original company_key if no identifiers
        groupKey = `key:${row.company_key}`;
      }
    }

    // Register all identifiers to this group
    if (row.orgnr) {
      identifierToGroup.set(`orgnr:${row.orgnr}`, groupKey);
    }
    if (row.clean_domain) {
      identifierToGroup.set(`clean_domain:${row.clean_domain}`, groupKey);
    }
    if (row.domain) {
      identifierToGroup.set(`domain:${row.domain}`, groupKey);
    }
    if (row.name_normalized) {
      identifierToGroup.set(`name:${row.name_normalized}`, groupKey);
    }

    if (!groups.has(groupKey)) {
      groups.set(groupKey, []);
    }
    groups.get(groupKey)!.push(row);
  }

  // Merge duplicates - keep best data from each
  const merged: CsvCompany[] = [];
  for (const [companyKey, duplicates] of groups) {
    if (duplicates.length === 1) {
      merged.push(duplicates[0]);
      continue;
    }

    // Merge: prefer rows with more complete data (orgnr > clean_domain > domain > name)
    const best = duplicates.reduce((best, current) => {
      const bestScore =
        (best.orgnr ? 4 : 0) +
        (best.clean_domain ? 3 : 0) +
        (best.domain ? 2 : 0) +
        (best.name ? 1 : 0);
      const currentScore =
        (current.orgnr ? 4 : 0) +
        (current.clean_domain ? 3 : 0) +
        (current.domain ? 2 : 0) +
        (current.name ? 1 : 0);

      if (currentScore > bestScore) return current;
      return best;
    });

    // Merge fields: take non-null values from all duplicates
    const mergedCompany: CsvCompany = {
      ...best,
      orgnr: best.orgnr || duplicates.find((d) => d.orgnr)?.orgnr || undefined,
      clean_domain:
        best.clean_domain ||
        duplicates.find((d) => d.clean_domain)?.clean_domain ||
        undefined,
      domain:
        best.domain || duplicates.find((d) => d.domain)?.domain || undefined,
      name: best.name || duplicates.find((d) => d.name)?.name || undefined,
      proff_url:
        best.proff_url ||
        duplicates.find((d) => d.proff_url)?.proff_url ||
        undefined,
      industry:
        best.industry ||
        duplicates.find((d) => d.industry)?.industry ||
        undefined,
      company_size:
        best.company_size ||
        duplicates.find((d) => d.company_size)?.company_size ||
        undefined,
      location:
        best.location ||
        duplicates.find((d) => d.location)?.location ||
        undefined,
      profit_before_tax:
        best.profit_before_tax ||
        duplicates.find((d) => d.profit_before_tax)?.profit_before_tax ||
        undefined,
      turnover:
        best.turnover ||
        duplicates.find((d) => d.turnover)?.turnover ||
        undefined,
    };

    merged.push(mergedCompany);
  }

  console.log(
    `Deduplicated ${rows.length} company rows -> ${merged.length} unique companies`
  );
  return merged;
}

async function upsertCompanies(rows: CsvCompany[]) {
  let collected: {
    id: string;
    company_key: string;
    domain: string | null;
    clean_domain: string | null;
    name: string | null;
  }[] = [];

  // Rebuild company keys using new priority: orgnr > clean_domain > domain > name
  const processedRows = rows.map((row) => {
    let companyKey: string;
    try {
      companyKey = buildCompanyKey({
        orgnr: row.orgnr,
        clean_domain: row.clean_domain,
        domain_host: row.domain,
        name: row.name,
      });
    } catch (error) {
      // If we can't build a key, use the existing one from CSV as fallback
      console.warn(
        `Cannot build company key for ${
          row.name || "unknown"
        }, using CSV key: ${row.company_key}`
      );
      companyKey = row.company_key;
    }

    return {
      ...row,
      company_key: companyKey,
    };
  });

  // Step 2: Deduplicate companies by company_key before upserting
  const deduplicatedRows = deduplicateCompanies(processedRows);

  for (const batch of chunk(deduplicatedRows, CHUNK)) {
    const sanitized = batch.map((row) =>
      pick(row, [
        "company_key",
        "name",
        "domain",
        "clean_domain",
        "orgnr",
        "proff_url",
        "industry",
        "company_size",
        "location",
        "profit_before_tax",
        "turnover",
      ])
    );
    const { data, error } = await supabase
      .from("companies")
      .upsert(sanitized, { onConflict: "company_key" })
      .select("id, company_key, domain, clean_domain, name");
    fail("upsert companies", error);
    collected = collected.concat(
      (data ?? []).map((r) => ({
        id: r.id as string,
        company_key: r.company_key as string,
        domain: (r.domain as string | null) ?? null,
        clean_domain: (r.clean_domain as string | null) ?? null,
        name: (r.name as string | null) ?? null,
      }))
    );
  }
  return collected;
}

/**
 * Find existing person by any identifier (LinkedIn, email, phone)
 */
async function findPersonByIdentifiers(params: {
  linkedin_url?: string | null;
  email?: string | null;
  phone?: string | null;
}): Promise<{ id: string; person_key: string } | null> {
  const { linkedin_url, email, phone } = params;
  const conditions: string[] = [];

  if (linkedin_url) {
    const normalizedLinkedIn = canonicalizeLinkedInUrl(linkedin_url);
    if (normalizedLinkedIn) {
      conditions.push(`linkedin_url.eq.${normalizedLinkedIn}`);
    }
  }
  if (email) {
    const normalizedEmail = normalizeEmail(email);
    if (normalizedEmail) {
      conditions.push(`email.eq.${normalizedEmail}`);
    }
  }
  if (phone) {
    const normalizedPhone = normalizePhone(phone);
    if (normalizedPhone) {
      conditions.push(`phone.eq.${normalizedPhone}`);
    }
  }

  if (conditions.length === 0) return null;

  const { data, error } = await supabase
    .from("people")
    .select("id, person_key")
    .or(conditions.join(","))
    .limit(1);

  if (error || !data || data.length === 0) return null;
  return { id: data[0].id as string, person_key: data[0].person_key as string };
}

/**
 * Normalize and deduplicate person rows based on identifiers
 */
function deduplicatePeople(rows: CsvPerson[]): CsvPerson[] {
  // First, normalize all identifiers
  const normalizedRows = rows.map((row) => ({
    ...row,
    email: normalizeEmail(row.email) || undefined,
    phone: normalizePhone(row.phone) || undefined,
    linkedin_url: canonicalizeLinkedInUrl(row.linkedin_url) || undefined,
    full_name: normalizeNameForComparison(row.full_name) || row.full_name,
  }));

  // Group by normalized identifiers - check LinkedIn, email, and phone
  // If any identifier matches, group them together
  const groups = new Map<string, CsvPerson[]>();
  const identifierToGroup = new Map<string, string>(); // identifier -> groupKey

  for (const row of normalizedRows) {
    // Find existing group by checking all identifiers
    let groupKey: string | undefined;

    // Check if any identifier already has a group
    if (row.linkedin_url) {
      groupKey = identifierToGroup.get(`linkedin:${row.linkedin_url}`);
    }
    if (!groupKey && row.email) {
      groupKey = identifierToGroup.get(`email:${row.email}`);
    }
    if (!groupKey && row.phone) {
      groupKey = identifierToGroup.get(`phone:${row.phone}`);
    }

    // If no existing group, create a new one
    if (!groupKey) {
      // Use the best identifier as the group key
      if (row.linkedin_url) {
        groupKey = `linkedin:${row.linkedin_url}`;
      } else if (row.email) {
        groupKey = `email:${row.email}`;
      } else if (row.phone) {
        groupKey = `phone:${row.phone}`;
      } else {
        // Use name + company as fallback
        const nameKey = normalizeNameForComparison(row.full_name) || "";
        const companyKey =
          normalizeNameForComparison(row.normalized_company_name) || "";
        groupKey = `name:${companyKey}_${nameKey}`;
      }
    }

    // Register all identifiers to this group
    if (row.linkedin_url) {
      identifierToGroup.set(`linkedin:${row.linkedin_url}`, groupKey);
    }
    if (row.email) {
      identifierToGroup.set(`email:${row.email}`, groupKey);
    }
    if (row.phone) {
      identifierToGroup.set(`phone:${row.phone}`, groupKey);
    }

    if (!groups.has(groupKey)) {
      groups.set(groupKey, []);
    }
    groups.get(groupKey)!.push(row);
  }

  // Merge duplicates - keep best data from each
  const merged: CsvPerson[] = [];
  for (const [groupKey, duplicates] of groups) {
    if (duplicates.length === 1) {
      merged.push(duplicates[0]);
      continue;
    }

    // Merge: prefer rows with more complete data
    const best = duplicates.reduce((best, current) => {
      const bestScore =
        (best.linkedin_url ? 4 : 0) +
        (best.email ? 2 : 0) +
        (best.phone ? 1 : 0) +
        (best.full_name ? 1 : 0);
      const currentScore =
        (current.linkedin_url ? 4 : 0) +
        (current.email ? 2 : 0) +
        (current.phone ? 1 : 0) +
        (current.full_name ? 1 : 0);

      if (currentScore > bestScore) return current;
      return best;
    });

    // Merge fields: take non-null values from all duplicates
    const mergedPerson: CsvPerson = {
      ...best,
      linkedin_url:
        best.linkedin_url ||
        duplicates.find((d) => d.linkedin_url)?.linkedin_url ||
        undefined,
      email: best.email || duplicates.find((d) => d.email)?.email || undefined,
      phone: best.phone || duplicates.find((d) => d.phone)?.phone || undefined,
      title: best.title || duplicates.find((d) => d.title)?.title || undefined,
      normalized_company_name:
        best.normalized_company_name ||
        duplicates.find((d) => d.normalized_company_name)
          ?.normalized_company_name ||
        undefined,
      normalized_company_domain:
        best.normalized_company_domain ||
        duplicates.find((d) => d.normalized_company_domain)
          ?.normalized_company_domain ||
        undefined,
    };

    merged.push(mergedPerson);
  }

  console.log(
    `Deduplicated ${rows.length} rows -> ${merged.length} unique people`
  );
  return merged;
}

async function upsertPeople(rows: CsvPerson[]) {
  let collected: {
    id: string;
    person_key: string;
    email: string | null;
    normalized_company_name: string | null;
    normalized_company_domain: string | null;
  }[] = [];

  // Step 1: Normalize and deduplicate rows
  const deduplicatedRows = deduplicatePeople(rows);

  // Step 2: Build correct keys (skip checking existing since we're doing clean import)
  const processedRows: CsvPerson[] = [];

  console.log(`Building person keys for ${deduplicatedRows.length} people...`);
  let processed = 0;

  for (const row of deduplicatedRows) {
    // Normalize identifiers before building key
    const normalizedLinkedIn = canonicalizeLinkedInUrl(row.linkedin_url);
    const normalizedEmail = normalizeEmail(row.email);
    const normalizedPhone = normalizePhone(row.phone);

    // Build the correct person_key using normalized identifiers
    let correctKey: string;
    try {
      correctKey = buildPersonKey({
        linkedin_url: normalizedLinkedIn,
        email: normalizedEmail,
        phone: normalizedPhone,
        company_domain: row.normalized_company_domain || undefined,
        company_key: null,
        company_name: row.normalized_company_name || undefined,
        full_name: row.full_name,
      });
    } catch (error) {
      console.warn(
        `Cannot build key for person ${row.full_name}, skipping:`,
        error
      );
      continue;
    }

    // Use normalized identifiers and correct key for upsert
    // Convert null to undefined for type compatibility
    processedRows.push({
      ...row,
      person_key: correctKey,
      email: normalizedEmail || undefined,
      phone: normalizedPhone || undefined,
      linkedin_url: normalizedLinkedIn || undefined,
    });

    processed++;
    if (processed % 500 === 0) {
      console.log(
        `Processed ${processed}/${deduplicatedRows.length} people...`
      );
    }
  }

  console.log(
    `Built keys for ${processedRows.length} people, starting upsert...`
  );

  // Upsert with correct keys (no need to update existing keys since we're doing clean import)
  for (const batch of chunk(processedRows, CHUNK)) {
    const sanitized = batch.map((row) =>
      pick(row, [
        "person_key",
        "full_name",
        "title",
        "email",
        "phone",
        "linkedin_url",
        "normalized_company_name",
        "normalized_company_domain",
      ])
    );

    const { data, error } = await supabase
      .from("people")
      .upsert(sanitized, { onConflict: "person_key" })
      .select(
        "id, person_key, email, normalized_company_name, normalized_company_domain"
      );
    fail("upsert people", error);
    collected = collected.concat(
      (data ?? []).map((r) => ({
        id: r.id as string,
        person_key: r.person_key as string,
        email: (r.email as string | null) ?? null,
        normalized_company_name:
          (r.normalized_company_name as string | null) ?? null,
        normalized_company_domain:
          (r.normalized_company_domain as string | null) ?? null,
      }))
    );
  }
  return collected;
}

async function upsertJobs(rows: CsvJob[], companyMap: Map<string, string>) {
  let collected: Map<string, string> = new Map();
  for (const batch of chunk(rows, CHUNK)) {
    const payload = batch.map((r) => ({
      finn_url: r.finn_url,
      company_id: companyMap.get(r.company_key)!,
      company_name_raw: r.company_name_raw,
      job_title: r.job_title,
      description: r.description,
      application_url: r.application_url,
      location: r.location,
      employment_type: r.employment_type,
      publication_date: r.publication_date,
      expiration_date_raw: r.expiration_date_raw,
      company_logo_url: r.company_logo_url,
      scraped_at: r.scraped_at,
      status: r.status ?? "new",
    }));
    const sanitized = payload.map((row) =>
      pick(row, [
        "finn_url",
        "company_id",
        "company_name_raw",
        "job_title",
        "description",
        "application_url",
        "location",
        "employment_type",
        "publication_date",
        "expiration_date_raw",
        "company_logo_url",
        "scraped_at",
        "status",
      ])
    );
    const { data, error } = await supabase
      .from("job_posts")
      .upsert(sanitized, { onConflict: "finn_url" })
      .select("id, finn_url");
    fail("upsert job_posts", error);
    for (const r of data ?? []) {
      collected.set(r.finn_url as string, r.id as string);
    }
  }
  return collected;
}

async function upsertJobPostPeopleFromContacts(
  rows: CsvJob[],
  jobMap: Map<string, string>,
  companyMap: Map<string, string>
) {
  const payload: {
    job_post_id: string;
    person_id: string;
    role: string;
    source: string | null;
  }[] = [];

  for (const job of rows) {
    const job_post_id = jobMap.get(job.finn_url);
    if (!job_post_id) continue;
    // Contact present?
    if (
      !job.contact_email &&
      !job.contact_phone &&
      !job.contact_linkedin &&
      !job.contact_name
    )
      continue;

    // Build person key using the new strategy (LinkedIn > Email > Phone > Name+Company)
    const email = job.contact_email?.toLowerCase().trim() || null;
    const phone = job.contact_phone?.replace(/\D+/g, "") || null;
    const linkedin = job.contact_linkedin?.trim() || null;
    const name = job.contact_name?.trim() || "Contact";

    // Get company name for name-based keys
    const companyName = job.company_name_raw || null;

    const person_key = buildPersonKey({
      linkedin_url: linkedin,
      email: email,
      phone: phone,
      company_key: job.company_key,
      company_name: companyName,
      full_name: name,
    });

    // Upsert person on the fly
    const personPayload = [
      {
        person_key,
        full_name: name,
        email,
        phone,
        linkedin_url: linkedin,
        title: job.contact_role ?? null,
      },
    ];

    const { data, error } = await supabase
      .from("people")
      .upsert(personPayload, { onConflict: "person_key" })
      .select("id, person_key");
    fail("upsert inline person", error);
    const person_id = data![0].id as string;

    payload.push({
      job_post_id,
      person_id,
      role: job.contact_role || "contact_person",
      source: "csv_contact",
    });

    // Also link to company_people if we can
    const company_id = companyMap.get(job.company_key);
    if (company_id) {
      const cp = [
        {
          company_id,
          person_id,
          role: job.contact_role || "contact_person",
        },
      ];
      const { error: cpErr } = await supabase
        .from("company_people")
        .upsert(cp, { onConflict: "company_id,person_id,role" });
      fail("upsert company_people (contact)", cpErr);
    }
  }

  if (payload.length) {
    const { error } = await supabase
      .from("job_post_people")
      .upsert(payload, { onConflict: "job_post_id,person_id,role" });
    fail("upsert job_post_people", error);
  }
}

async function upsertCompanyPeopleBySignals(
  companies: {
    id: string;
    company_key: string;
    domain: string | null;
    clean_domain: string | null;
    name: string | null;
  }[],
  people: {
    id: string;
    person_key: string;
    email: string | null;
    normalized_company_name: string | null;
    normalized_company_domain: string | null;
  }[]
) {
  // Build domain lookup (prefer clean_domain, fall back to domain)
  const byDomain = new Map<string, string>();
  for (const c of companies) {
    const doms = [
      normalizeDomain(c.clean_domain),
      normalizeDomain(c.domain),
    ].filter(Boolean) as string[];
    for (const d of doms) byDomain.set(d, c.id);
  }

  // Build normalized name lookup derived from company name
  const byName = new Map<string, string>();
  for (const c of companies) {
    const n = normalizeName(c.name);
    if (n) byName.set(n, c.id);
  }

  const payload: { company_id: string; person_id: string; role: string }[] = [];
  for (const p of people) {
    const dom =
      normalizeDomain(emailDomain(p.email || undefined)) ||
      normalizeDomain(p.normalized_company_domain || undefined);
    const companyByDomain = dom ? byDomain.get(dom) : undefined;

    const normName = normalizeName(p.normalized_company_name);
    const companyByName = normName ? byName.get(normName) : undefined;

    const company_id = companyByDomain || companyByName;
    if (!company_id) continue;

    payload.push({ company_id, person_id: p.id, role: "contact_person" });
  }

  for (const batch of chunk(payload, CHUNK)) {
    const { error } = await supabase
      .from("company_people")
      .upsert(batch, { onConflict: "company_id,person_id,role" });
    fail("upsert company_people (signals)", error);
  }
}

async function main() {
  // Read all companies CSV files (companies.csv, companies_2.csv, etc.)
  const companies = readMultipleCsv<CsvCompany>(/^companies.*\.csv$/);

  // Read all people CSV files (people.csv, people_2.csv, etc.)
  const people = readMultipleCsv<CsvPerson>(/^people.*\.csv$/);

  // Skip historic job_posts import for now due to missing company_id mapping.
  const jobs: CsvJob[] = [];

  const companyRecords = await upsertCompanies(companies);
  const companyMap = new Map(
    companyRecords.map((c) => [c.company_key, c.id] as const)
  );

  const peopleRecords = await upsertPeople(people);
  const personMap = new Map(
    peopleRecords.map((p) => [p.person_key, p.id] as const)
  );

  // No job_posts import. Only people/company data and relations.

  // Auto-create company_people using domain + normalized-name signals
  await upsertCompanyPeopleBySignals(companyRecords, peopleRecords);

  console.log("Import completed with auto-generated relations where possible");
}

main().catch((err) => {
  console.error("Import failed:", err);
  try {
    console.error("As JSON:", JSON.stringify(err, null, 2));
  } catch {
    /* noop */
  }
  process.exit(1);
});
