import { FastifyInstance, FastifyPluginOptions } from "fastify";
import { z } from "zod";
import {
  extractFinnIdFromUrl,
  normalizeDomainHost,
  normalizePhone,
  normalizeEmail,
  normalizeDate,
  classifyPersonRole,
  normalizeCompanyNameForMatching,
} from "../lib/normalize";
import { buildCompanyKey, buildPersonKey } from "../lib/keys";
import {
  upsertCompanySmart,
  upsertJobPosts,
  upsertPeople,
  upsertJobPostPeople,
  upsertCompanyPeople,
  getDecisionMakersByCompanyId,
  getJobPostWithDecisionMakers,
  CompanyRecord,
  PersonRecord,
  JobPostRecord,
} from "../lib/db";
import {
  sendToClayWebhook,
  isClayWebhookEnabled,
  ClayJobPostPayload,
} from "../lib/clay";

const apifySchema = z.object({
  url: z.string().url(),
  title: z.string(),
  description: z.string(),
  company: z.string(),
  contactPersons: z
    .array(
      z.object({
        name: z.string(),
        role: z.string().optional(),
        phoneNumber: z.string().optional(),
        email: z.string().optional(),
        linkedin: z.string().optional(),
      })
    )
    .default([]),
  applicationUrl: z.string().url().optional(),
  location: z.string().optional(),
  employmentType: z.string().optional(),
  email: z.string().optional(),
  salary: z.string().optional(),
  publicationDate: z.string().optional(),
  expirationDate: z.string().optional(),
  companyLogoUrl: z.string().url().optional(),
  domain: z.string().optional(),
  sector: z.string().optional(),
  industries: z.array(z.string()).optional(),
  positionFunctions: z.array(z.string()).optional(),
  language: z.string().optional(),
  finnkode: z.string().optional(),
});

export async function handleApifyJob(
  request: any,
  reply: any,
  source: string | null
) {
  const parsed = apifySchema.safeParse(request.body);
  if (!parsed.success) {
    reply
      .status(400)
      .send({ error: "Bad Request", message: parsed.error.message });
    return;
  }

  const payload = parsed.data;
  const finnId = extractFinnIdFromUrl(payload.url) || payload.finnkode || null;
  if (!finnId) {
    reply.status(400).send({
      error: "Bad Request",
      message: "Unable to extract finn_id from url",
    });
    return;
  }

  // Extract domain, but prefer payload.domain over extracting from URL
  // If domain is missing or invalid, buildCompanyKey will fall back to company name
  const companyDomain = payload.domain
    ? normalizeDomainHost(payload.domain)
    : null;

  const companyKey = buildCompanyKey({
    domain_host: companyDomain,
    name: payload.company,
  });

  // Build company record with all available data from scraper
  // Only include fields that have values to preserve existing data on updates
  const company: CompanyRecord = {
    company_key: companyKey,
    name: payload.company,
    ...(companyDomain && { domain: companyDomain }),
    ...(companyDomain && { clean_domain: companyDomain }), // clean_domain is same as normalized domain
    ...(payload.sector && { sector: payload.sector }),
    ...(payload.industries &&
      payload.industries.length > 0 && { industry: payload.industries[0] }), // Use first industry if available
    ...(payload.location && { location: payload.location }),
  };

  // Upsert company using smart matching (checks orgnr, clean_domain, name)
  const companyResult = await upsertCompanySmart(company);
  const companyId = companyResult.id;
  const actualCompanyKey = companyResult.company_key; // May differ from buildCompanyKey if matched existing

  const job: JobPostRecord = {
    finn_id: finnId,
    company_id: companyId,
    finn_url: payload.url,
    title: payload.title,
    description: payload.description,
    application_url: payload.applicationUrl ?? null,
    contact_email: payload.email ?? null,
    location: payload.location ?? null,
    employment_type: payload.employmentType ?? null,
    salary: payload.salary ?? null,
    publication_date: normalizeDate(payload.publicationDate),
    expiration_date: normalizeDate(payload.expirationDate),
    sector: payload.sector ?? null,
    industries: payload.industries ?? null,
    position_functions: payload.positionFunctions ?? null,
    language: payload.language ?? null,
    company_logo_url: payload.companyLogoUrl ?? null,
    company_name: payload.company,
    company_domain_host: companyDomain,
    source: source,
    raw_payload: payload,
  };

  // Upsert job post and get its ID
  const jobResult = await upsertJobPosts([job]);
  const jobPostId = jobResult.records[0]?.id;
  if (!jobPostId) {
    reply.status(500).send({
      error: "Internal Server Error",
      message: "Failed to get job post ID after upsert",
    });
    return;
  }

  const people: PersonRecord[] = payload.contactPersons.map((p) => {
    const personKey = buildPersonKey({
      linkedin_url: p.linkedin,
      email: p.email,
      phone: p.phoneNumber,
      company_key: actualCompanyKey, // Use actual company key (may be from existing record)
      company_name: payload.company, // Pass company name for name-based keys
      full_name: p.name,
    });

    // Normalize company name for people record (same function used for companies.clean_name)
    const normalizedCompanyName = normalizeCompanyNameForMatching(payload.company);

    return {
      person_key: personKey,
      full_name: p.name,
      title: p.role ?? null,
      phone: normalizePhone(p.phoneNumber),
      email: normalizeEmail(p.email),
      linkedin_url: p.linkedin ?? null,
      ...(normalizedCompanyName && {
        normalized_company_name: normalizedCompanyName,
      }),
      ...(companyDomain && { normalized_company_domain: companyDomain }),
    };
  });

  // Upsert people and get their IDs
  const peopleResult = await upsertPeople(people);
  const personIdMap = new Map<string, string>();
  for (const person of peopleResult.records) {
    if (person.person_key && person.id) {
      personIdMap.set(person.person_key, person.id);
    }
  }

  // Build link records with UUIDs, using role classification based on title
  const jobPersonLinks = people
    .map((p, index) => {
      const personId = personIdMap.get(p.person_key);
      if (!personId) return null;

      // Get the original person data to access their role/title
      const originalPerson = payload.contactPersons[index];
      const classifiedRole = classifyPersonRole(originalPerson?.role);

      return {
        job_post_id: jobPostId,
        person_id: personId,
        role: classifiedRole,
      };
    })
    .filter((link): link is NonNullable<typeof link> => link !== null);

  const companyPersonLinks = people
    .map((p, index) => {
      const personId = personIdMap.get(p.person_key);
      if (!personId) return null;

      // Get the original person data to access their role/title
      const originalPerson = payload.contactPersons[index];
      const classifiedRole = classifyPersonRole(originalPerson?.role);

      return {
        company_id: companyId,
        person_id: personId,
        role: classifiedRole,
      };
    })
    .filter((link): link is NonNullable<typeof link> => link !== null);

  // Enrich job post with decision makers from the company
  // But exclude decision makers who are already linked as decision_maker to avoid duplicates
  const decisionMakerIds = await getDecisionMakersByCompanyId(companyId);
  const existingDecisionMakerPersonIds = new Set(
    jobPersonLinks
      .filter((link) => link.role === "decision_maker")
      .map((link) => link.person_id)
  );
  const decisionMakerLinks = decisionMakerIds
    .filter((personId) => !existingDecisionMakerPersonIds.has(personId))
    .map((personId) => ({
      job_post_id: jobPostId,
      person_id: personId,
      role: "decision_maker" as const,
    }));

  // Combine contact persons and decision makers for job_post_people
  const allJobPersonLinks = [...jobPersonLinks, ...decisionMakerLinks];

  const results = {
    companies: {
      inserted: companyResult.isNew ? 1 : 0,
      updated: companyResult.isNew ? 0 : 1,
      matched_existing: !companyResult.isNew,
    },
    job_posts: jobResult,
    people: peopleResult,
    job_post_people: await upsertJobPostPeople(allJobPersonLinks),
    company_people: await upsertCompanyPeople(companyPersonLinks),
    decision_makers_linked: decisionMakerLinks.length,
  };

  // Send to Clay webhook (fire-and-forget, don't block the response)
  if (isClayWebhookEnabled()) {
    // Use setImmediate to not block the response
    setImmediate(async () => {
      try {
        const enrichedJobPost = await getJobPostWithDecisionMakers(jobPostId);
        if (enrichedJobPost) {
          const clayPayload: ClayJobPostPayload = {
            job_post: {
              finn_id: enrichedJobPost.job_post.finn_id,
              finn_url: enrichedJobPost.job_post.finn_url,
              title: enrichedJobPost.job_post.title,
              description: enrichedJobPost.job_post.description,
              location: enrichedJobPost.job_post.location,
              employment_type: enrichedJobPost.job_post.employment_type,
              salary: enrichedJobPost.job_post.salary,
              publication_date: enrichedJobPost.job_post.publication_date,
              expiration_date: enrichedJobPost.job_post.expiration_date,
              application_url: enrichedJobPost.job_post.application_url,
              sector: enrichedJobPost.job_post.sector,
              industries: enrichedJobPost.job_post.industries,
              source: enrichedJobPost.job_post.source,
            },
            company: {
              name: enrichedJobPost.company.name,
              domain: enrichedJobPost.company.domain,
              clean_domain: enrichedJobPost.company.clean_domain,
              orgnr: enrichedJobPost.company.orgnr,
              proff_url: enrichedJobPost.company.proff_url,
              industry: enrichedJobPost.company.industry,
              company_size: enrichedJobPost.company.company_size,
              location: enrichedJobPost.company.location,
              sector: enrichedJobPost.company.sector,
              profit_before_tax: enrichedJobPost.company.profit_before_tax,
              turnover: enrichedJobPost.company.turnover,
            },
            decision_makers: enrichedJobPost.decision_makers.map((dm) => ({
              full_name: dm.full_name,
              title: dm.title,
              email: dm.email,
              phone: dm.phone,
              linkedin_url: dm.linkedin_url,
            })),
          };
          await sendToClayWebhook(clayPayload);
        }
      } catch (error) {
        console.error("[Clay] Error sending webhook:", error);
      }
    });
  }

  reply.send(results);
}

export default async function ingestRoutes(
  app: FastifyInstance,
  _opts: FastifyPluginOptions
) {
  // Original endpoint for backward compatibility (source defaults to null)
  app.post("/apify-job", async (request, reply) => {
    await handleApifyJob(request, reply, null);
  });

  // SYSTEK endpoint
  app.post("/apify-job-systek", async (request, reply) => {
    await handleApifyJob(request, reply, "systek");
  });

  // ILDER endpoint
  app.post("/apify-job-ilder", async (request, reply) => {
    await handleApifyJob(request, reply, "ilder");
  });
}
