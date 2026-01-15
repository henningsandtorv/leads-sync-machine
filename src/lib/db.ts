import { supabase } from "./supabase";
import {
  normalizeDomainHost,
  nameSlug,
  normalizeOrgnr,
  normalizeCompanyNameForMatching,
} from "./normalize";

export type CompanyRecord = {
  company_key: string;
  name: string;
  domain?: string | null;
  clean_domain?: string | null;
  clean_name?: string | null;
  orgnr?: string | null;
  proff_url?: string | null;
  industry?: string | null;
  company_size?: string | null;
  location?: string | null;
  sector?: string | null;
};

export type PersonRecord = {
  person_key: string;
  full_name: string;
  title?: string | null;
  email?: string | null;
  phone?: string | null;
  linkedin_url?: string | null;
  normalized_company_name?: string | null;
  normalized_company_domain?: string | null;
};

export type JobPostRecord = {
  finn_id: string;
  company_id: string; // UUID
  finn_url: string;
  title?: string | null;
  description?: string | null;
  application_url?: string | null;
  contact_email?: string | null;
  location?: string | null;
  employment_type?: string | null;
  salary?: string | null;
  publication_date?: string | null;
  expiration_date?: string | null;
  sector?: string | null;
  industries?: string[] | null;
  position_functions?: string[] | null;
  language?: string | null;
  company_logo_url?: string | null;
  company_name?: string | null;
  company_domain_host?: string | null;
  source?: string | null;
  raw_payload?: unknown;
};

type LinkRecord = { role: string };
type JobPostPersonRecord = LinkRecord & {
  job_post_id: string;
  person_id: string;
}; // UUIDs
type CompanyPersonRecord = LinkRecord & {
  company_id: string; // UUID
  person_id: string; // UUID
};

async function selectExistingKeys(
  table: string,
  column: string,
  keys: string[]
) {
  if (!keys.length) return new Set<string>();
  const { data, error } = await supabase
    .from(table)
    .select(column)
    .in(column, keys);
  if (error) throw error;
  return new Set((data ?? []).map((row: any) => row[column] as string));
}

async function upsertWithCounts<T extends { [k: string]: any }>(params: {
  table: string;
  records: T[];
  keyColumn: string;
}) {
  const { table, records, keyColumn } = params;
  if (!records.length) return { inserted: 0, updated: 0, records: [] };
  const keys = records.map((r) => r[keyColumn]) as string[];
  const existing = await selectExistingKeys(table, keyColumn, keys);
  const insertedKeys = keys.filter((k) => !existing.has(k));
  const updated = keys.length - insertedKeys.length;
  const { data, error } = await supabase
    .from(table)
    .upsert(records, { onConflict: keyColumn })
    .select();
  if (error) throw error;
  return {
    inserted: insertedKeys.length,
    updated,
    records: (data ?? []) as (T & { id: string })[],
  };
}

/**
 * Find an existing company by checking multiple fields in priority order:
 * 1. orgnr (if provided and matches)
 * 2. clean_domain (if provided and matches)
 * 3. clean_name (normalized company name without legal suffixes like AS/ASA/Ltd)
 * 4. company_key matching the name slug (legacy fallback)
 *
 * Returns the existing company's ID and company_key if found, null otherwise.
 */
export async function findExistingCompany(params: {
  orgnr?: string | null;
  domain?: string | null;
  clean_domain?: string | null;
  name?: string | null;
}): Promise<{ id: string; company_key: string } | null> {
  const { orgnr, domain, clean_domain, name } = params;

  // Normalize inputs for matching
  const normalizedOrgnr = normalizeOrgnr(orgnr);
  const normalizedDomain = normalizeDomainHost(clean_domain || domain);
  const normalizedCleanName = normalizeCompanyNameForMatching(name);
  const normalizedNameSlug = nameSlug(name);

  // Priority 1: Check by orgnr
  if (normalizedOrgnr) {
    const { data, error } = await supabase
      .from("companies")
      .select("id, company_key")
      .eq("orgnr", normalizedOrgnr)
      .limit(1)
      .single();
    // PGRST116 = no rows returned, which is expected when not found
    if (error && error.code !== "PGRST116") throw error;
    if (data) {
      return { id: data.id, company_key: data.company_key };
    }
  }

  // Priority 2: Check by clean_domain
  if (normalizedDomain && normalizedDomain !== "finn.no") {
    const { data, error } = await supabase
      .from("companies")
      .select("id, company_key")
      .eq("clean_domain", normalizedDomain)
      .limit(1)
      .single();
    if (error && error.code !== "PGRST116") throw error;
    if (data) {
      return { id: data.id, company_key: data.company_key };
    }
  }

  // Priority 3: Check by clean_name (handles variations like "AKVA Group" vs "AKVA Group ASA")
  if (normalizedCleanName) {
    const { data, error } = await supabase
      .from("companies")
      .select("id, company_key")
      .eq("clean_name", normalizedCleanName)
      .limit(1)
      .single();
    if (error && error.code !== "PGRST116") throw error;
    if (data) {
      return { id: data.id, company_key: data.company_key };
    }
  }

  // Priority 4: Check by company_key matching the name slug (legacy fallback)
  if (normalizedNameSlug) {
    const { data, error } = await supabase
      .from("companies")
      .select("id, company_key")
      .eq("company_key", normalizedNameSlug)
      .limit(1)
      .single();
    if (error && error.code !== "PGRST116") throw error;
    if (data) {
      return { id: data.id, company_key: data.company_key };
    }
  }

  return null;
}

/**
 * Smart upsert for a single company that checks multiple fields for existing matches.
 * If a match is found, updates the existing record. Otherwise, creates a new one.
 * Uses atomic operations to handle race conditions.
 */
export async function upsertCompanySmart(
  record: CompanyRecord
): Promise<{ id: string; company_key: string; isNew: boolean }> {
  // Compute clean_name for matching and storage
  const cleanName = normalizeCompanyNameForMatching(record.name);

  // First, check if this company already exists under a different key
  const existing = await findExistingCompany({
    orgnr: record.orgnr,
    domain: record.domain,
    clean_domain: record.clean_domain,
    name: record.name,
  });

  if (existing) {
    // Update the existing company record, preserving its company_key
    // Only update fields that have values (don't overwrite with nulls)
    const updateData: Partial<CompanyRecord> = {};
    if (record.name) updateData.name = record.name;
    if (record.domain) updateData.domain = record.domain;
    if (record.clean_domain) updateData.clean_domain = record.clean_domain;
    if (cleanName) updateData.clean_name = cleanName;
    if (record.orgnr) updateData.orgnr = record.orgnr;
    if (record.proff_url) updateData.proff_url = record.proff_url;
    if (record.industry) updateData.industry = record.industry;
    if (record.company_size) updateData.company_size = record.company_size;
    if (record.location) updateData.location = record.location;
    if (record.sector) updateData.sector = record.sector;

    if (Object.keys(updateData).length > 0) {
      const { error: updateError } = await supabase
        .from("companies")
        .update(updateData)
        .eq("id", existing.id);
      if (updateError) throw updateError;
    }

    return { id: existing.id, company_key: existing.company_key, isNew: false };
  }

  // No existing match found, insert new company with clean_name
  // Use upsert to handle race condition where another request inserted the same company_key
  const recordWithCleanName = {
    ...record,
    ...(cleanName && { clean_name: cleanName }),
  };

  const { data, error } = await supabase
    .from("companies")
    .upsert([recordWithCleanName], { onConflict: "company_key" })
    .select("id, company_key")
    .single();

  if (error) throw error;

  // Check if this was actually an insert or update by comparing timestamps
  // Since we already checked findExistingCompany and it returned null,
  // if we get here via upsert conflict, it means a race condition occurred
  // We return isNew: true since from this request's perspective, it was attempting to create
  return { id: data.id, company_key: data.company_key, isNew: true };
}

export async function upsertCompanies(records: CompanyRecord[]) {
  return upsertWithCounts<CompanyRecord>({
    table: "companies",
    records,
    keyColumn: "company_key",
  });
}

export async function upsertPeople(records: PersonRecord[]) {
  return upsertWithCounts<PersonRecord>({
    table: "people",
    records,
    keyColumn: "person_key",
  });
}

export async function upsertJobPosts(records: JobPostRecord[]) {
  return upsertWithCounts<JobPostRecord>({
    table: "job_posts",
    records,
    keyColumn: "finn_id",
  });
}

/**
 * Smart upsert for a single job post that appends sources instead of overwriting.
 * If job exists and has a different source, combines them as "source1,source2".
 * Uses atomic upsert with conflict handling to prevent race conditions.
 */
export async function upsertJobPostSmart(
  record: JobPostRecord
): Promise<{ id: string; isNew: boolean }> {
  // Check if job post already exists
  const { data: existing, error: fetchError } = await supabase
    .from("job_posts")
    .select("id, source")
    .eq("finn_id", record.finn_id)
    .single();

  if (fetchError && fetchError.code !== "PGRST116") {
    // PGRST116 = no rows returned, which is fine
    throw fetchError;
  }

  if (existing) {
    // Job post exists - merge sources if different
    const existingSources = new Set(
      (existing.source || "").split(",").map((s: string) => s.trim()).filter(Boolean)
    );
    const newSource = record.source?.trim();

    if (newSource && !existingSources.has(newSource)) {
      existingSources.add(newSource);
    }

    const mergedSource = Array.from(existingSources).sort().join(",") || null;

    // Update with merged source (only update source, preserve other data)
    const { error: updateError } = await supabase
      .from("job_posts")
      .update({ source: mergedSource })
      .eq("id", existing.id);

    if (updateError) throw updateError;

    return { id: existing.id, isNew: false };
  }

  // New job post - use upsert to handle race condition where another request
  // may have inserted the same finn_id between our check and insert
  const { data, error } = await supabase
    .from("job_posts")
    .upsert([record], { onConflict: "finn_id" })
    .select("id")
    .single();

  if (error) throw error;
  return { id: data.id, isNew: true };
}

// Helper functions to look up IDs from keys
export async function getCompanyIdByKey(
  companyKey: string
): Promise<string | null> {
  const { data, error } = await supabase
    .from("companies")
    .select("id")
    .eq("company_key", companyKey)
    .single();
  // PGRST116 = no rows returned, which means not found
  if (error && error.code !== "PGRST116") throw error;
  if (!data) return null;
  return data.id as string;
}

export async function getPersonIdByKey(
  personKey: string
): Promise<string | null> {
  const { data, error } = await supabase
    .from("people")
    .select("id")
    .eq("person_key", personKey)
    .single();
  if (error && error.code !== "PGRST116") throw error;
  if (!data) return null;
  return data.id as string;
}

export async function getJobPostIdByFinnId(
  finnId: string
): Promise<string | null> {
  const { data, error } = await supabase
    .from("job_posts")
    .select("id")
    .eq("finn_id", finnId)
    .single();
  if (error && error.code !== "PGRST116") throw error;
  if (!data) return null;
  return data.id as string;
}

/**
 * Get all decision makers for a company
 */
export async function getDecisionMakersByCompanyId(
  companyId: string
): Promise<string[]> {
  const { data, error } = await supabase
    .from("company_people")
    .select("person_id")
    .eq("company_id", companyId)
    .eq("role", "decision_maker");
  if (error) throw error;
  if (!data) return [];
  return data.map((row) => row.person_id as string);
}

async function upsertLinkTable<T extends LinkRecord>(params: {
  table: string;
  records: T[];
  keyBuilder: (row: T) => string;
  filters: Record<string, string[]>;
}) {
  const { table, records, keyBuilder, filters } = params;
  if (!records.length) return { inserted: 0, existing: 0 };

  // Deduplicate records within the batch to avoid "cannot affect row a second time" errors
  const seenKeys = new Set<string>();
  const deduplicatedRecords: T[] = [];
  for (const record of records) {
    const key = keyBuilder(record);
    if (!seenKeys.has(key)) {
      seenKeys.add(key);
      deduplicatedRecords.push(record);
    }
  }

  const { data, error } = await Object.entries(filters).reduce(
    (query, [column, values]) => query.in(column, values),
    supabase.from(table).select("*")
  );
  if (error) throw error;
  const existingKeys = new Set((data ?? []).map(keyBuilder));
  const incomingKeys = new Set(deduplicatedRecords.map(keyBuilder));
  const newRecords = deduplicatedRecords.filter(
    (r) => !existingKeys.has(keyBuilder(r))
  );
  const { error: upsertError } = await supabase
    .from(table)
    .upsert(deduplicatedRecords);
  if (upsertError) throw upsertError;
  return {
    inserted: newRecords.length,
    existing: incomingKeys.size - newRecords.length,
  };
}

export async function upsertJobPostPeople(records: JobPostPersonRecord[]) {
  return upsertLinkTable<JobPostPersonRecord>({
    table: "job_post_people",
    records,
    keyBuilder: (r) => `${r.job_post_id}::${r.person_id}::${r.role}`,
    filters: {
      job_post_id: Array.from(new Set(records.map((r) => r.job_post_id))),
      person_id: Array.from(new Set(records.map((r) => r.person_id))),
    },
  });
}

export async function upsertCompanyPeople(records: CompanyPersonRecord[]) {
  return upsertLinkTable<CompanyPersonRecord>({
    table: "company_people",
    records,
    keyBuilder: (r) => `${r.company_id}::${r.person_id}::${r.role}`,
    filters: {
      company_id: Array.from(new Set(records.map((r) => r.company_id))),
      person_id: Array.from(new Set(records.map((r) => r.person_id))),
    },
  });
}

/**
 * Enriched job post with company and decision makers for Clay webhook
 */
export type EnrichedJobPost = {
  job_post: {
    id: string;
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
    created_at: string | null;
  };
  company: {
    id: string;
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
  decision_makers: Array<{
    id: string;
    full_name: string;
    title: string | null;
    email: string | null;
    phone: string | null;
    linkedin_url: string | null;
  }>;
  contact_persons: Array<{
    id: string;
    full_name: string;
    title: string | null;
    email: string | null;
    phone: string | null;
    linkedin_url: string | null;
  }>;
};

/**
 * Get a job post with its company and all linked decision makers
 */
export async function getJobPostWithDecisionMakers(
  jobPostId: string
): Promise<EnrichedJobPost | null> {
  // Fetch job post with company
  const { data: jobPost, error: jobError } = await supabase
    .from("job_posts")
    .select(
      `
      id,
      finn_id,
      finn_url,
      title,
      description,
      location,
      employment_type,
      salary,
      publication_date,
      expiration_date,
      application_url,
      sector,
      industries,
      source,
      created_at,
      companies (
        id,
        name,
        domain,
        clean_domain,
        orgnr,
        proff_url,
        industry,
        company_size,
        location,
        sector,
        profit_before_tax,
        turnover
      )
    `
    )
    .eq("id", jobPostId)
    .single();

  if (jobError || !jobPost) {
    console.error("[DB] Error fetching job post:", jobError);
    return null;
  }

  // Fetch decision makers linked to this job post
  const { data: decisionMakerLinks, error: dmError } = await supabase
    .from("job_post_people")
    .select(
      `
      people (
        id,
        full_name,
        title,
        email,
        phone,
        linkedin_url
      )
    `
    )
    .eq("job_post_id", jobPostId)
    .eq("role", "decision_maker");

  if (dmError) {
    console.error("[DB] Error fetching decision makers:", dmError);
    return null;
  }

  // Fetch contact persons linked to this job post
  const { data: contactPersonLinks, error: cpError } = await supabase
    .from("job_post_people")
    .select(
      `
      people (
        id,
        full_name,
        title,
        email,
        phone,
        linkedin_url
      )
    `
    )
    .eq("job_post_id", jobPostId)
    .eq("role", "contact_person");

  if (cpError) {
    console.error("[DB] Error fetching contact persons:", cpError);
    return null;
  }

  const company = jobPost.companies as any;
  const decisionMakers = (decisionMakerLinks ?? [])
    .map((link: any) => link.people)
    .filter(Boolean);
  const contactPersons = (contactPersonLinks ?? [])
    .map((link: any) => link.people)
    .filter(Boolean);

  return {
    job_post: {
      id: jobPost.id,
      finn_id: jobPost.finn_id,
      finn_url: jobPost.finn_url,
      title: jobPost.title,
      description: jobPost.description,
      location: jobPost.location,
      employment_type: jobPost.employment_type,
      salary: jobPost.salary,
      publication_date: jobPost.publication_date,
      expiration_date: jobPost.expiration_date,
      application_url: jobPost.application_url,
      sector: jobPost.sector,
      industries: jobPost.industries,
      source: jobPost.source,
      created_at: jobPost.created_at,
    },
    company: {
      id: company?.id ?? "",
      name: company?.name ?? null,
      domain: company?.domain ?? null,
      clean_domain: company?.clean_domain ?? null,
      orgnr: company?.orgnr ?? null,
      proff_url: company?.proff_url ?? null,
      industry: company?.industry ?? null,
      company_size: company?.company_size ?? null,
      location: company?.location ?? null,
      sector: company?.sector ?? null,
      profit_before_tax: company?.profit_before_tax ?? null,
      turnover: company?.turnover ?? null,
    },
    decision_makers: decisionMakers,
    contact_persons: contactPersons,
  };
}

/**
 * Get recent job posts (for batch sync)
 */
export async function getRecentJobPostIds(
  hoursAgo: number = 24
): Promise<string[]> {
  const since = new Date(Date.now() - hoursAgo * 60 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from("job_posts")
    .select("id")
    .gte("created_at", since)
    .order("created_at", { ascending: false });

  if (error) {
    console.error("[DB] Error fetching recent job posts:", error);
    return [];
  }

  return (data ?? []).map((row) => row.id);
}
