import { supabase } from "./supabase";

export type CompanyRecord = {
  company_key: string;
  name: string;
  domain?: string | null;
  clean_domain?: string | null;
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

// Helper functions to look up IDs from keys
export async function getCompanyIdByKey(
  companyKey: string
): Promise<string | null> {
  const { data, error } = await supabase
    .from("companies")
    .select("id")
    .eq("company_key", companyKey)
    .single();
  if (error || !data) return null;
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
  if (error || !data) return null;
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
  if (error || !data) return null;
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
  if (error || !data) return [];
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
