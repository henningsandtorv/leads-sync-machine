import { FastifyInstance, FastifyPluginOptions } from "fastify";
import { z } from "zod";
import {
  ClayEnrichmentPayloadSchema,
  ClayEnrichmentPayload,
} from "../lib/clay";
import {
  getCompanyByJobPostFinnId,
  getJobPostIdByFinnId,
  updateCompanyEnrichment,
  updatePersonEnrichment,
  findExistingPerson,
  upsertPeople,
  upsertCompanyPeople,
  upsertJobPostPeople,
  PersonRecord,
} from "../lib/db";
import {
  normalizeEmail,
  normalizePhone,
  normalizeOrgnr,
  normalizeCompanyNameForMatching,
  normalizeDomainHost,
  canonicalizeLinkedInUrl,
} from "../lib/normalize";
import { buildPersonKey } from "../lib/keys";

const CLAY_ENRICHMENT_SECRET = process.env.CLAY_ENRICHMENT_SECRET;

type EnrichmentStats = {
  finn_id: string;
  company: {
    found: boolean;
    fieldsUpdated: string[];
  };
  decision_makers: {
    added: number;
    existing: number;
    fieldsUpdated: string[];
  };
  contact_persons: {
    updated: number;
    fieldsUpdated: string[];
  };
  errors: string[];
};

async function handleClayEnrichment(
  payload: ClayEnrichmentPayload
): Promise<EnrichmentStats> {
  const stats: EnrichmentStats = {
    finn_id: payload.finn_id,
    company: { found: false, fieldsUpdated: [] },
    decision_makers: { added: 0, existing: 0, fieldsUpdated: [] },
    contact_persons: { updated: 0, fieldsUpdated: [] },
    errors: [],
  };

  try {
    // Step 1: Find the company via finn_id
    const company = await getCompanyByJobPostFinnId(payload.finn_id);
    if (!company) {
      stats.errors.push(`No company found for finn_id: ${payload.finn_id}`);
      return stats;
    }
    stats.company.found = true;

    const companyId = company.id;
    const jobPostId = await getJobPostIdByFinnId(payload.finn_id);

    // Step 2: Update company with enriched data
    if (payload.company) {
      const companyEnrichment = {
        orgnr: normalizeOrgnr(payload.company.orgnr),
        domain: normalizeDomainHost(payload.company.domain),
        clean_domain: normalizeDomainHost(payload.company.domain),
        proff_url: payload.company.proff_url || undefined,
        industry: payload.company.industry || undefined,
        company_size: payload.company.company_size || undefined,
        location: payload.company.location || undefined,
        sector: payload.company.sector || undefined,
        profit_before_tax: payload.company.profit_before_tax || undefined,
        turnover: payload.company.turnover || undefined,
      };

      const companyResult = await updateCompanyEnrichment(
        companyId,
        companyEnrichment
      );
      stats.company.fieldsUpdated = companyResult.fieldsUpdated;
    }

    // Step 3: Add new decision makers
    if (payload.decision_makers.length > 0) {
      const newPeople: PersonRecord[] = [];

      for (const dm of payload.decision_makers) {
        // Check if this person already exists
        const existing = await findExistingPerson({
          linkedin_url: dm.linkedin_url,
          email: dm.email,
          phone: dm.phone,
          company_id: companyId,
          full_name: dm.full_name,
        });

        if (existing) {
          stats.decision_makers.existing++;
          // Ensure decision maker is linked to company/job post
          await upsertCompanyPeople([
            {
              company_id: companyId,
              person_id: existing.id,
              role: "decision_maker" as const,
            },
          ]);
          if (jobPostId) {
            await upsertJobPostPeople([
              {
                job_post_id: jobPostId,
                person_id: existing.id,
                role: "decision_maker" as const,
              },
            ]);
          }
          // Update existing person with any new data
          const updateResult = await updatePersonEnrichment(existing.id, {
            title: dm.title,
            email: normalizeEmail(dm.email),
            phone: normalizePhone(dm.phone),
            linkedin_url: canonicalizeLinkedInUrl(dm.linkedin_url),
          });
          if (updateResult.fieldsUpdated.length > 0) {
            stats.decision_makers.fieldsUpdated.push(
              ...updateResult.fieldsUpdated.map((f) => `${dm.full_name}:${f}`)
            );
          }
        } else {
          // Build person record for insertion
          const personKey = buildPersonKey({
            linkedin_url: dm.linkedin_url,
            email: dm.email,
            phone: dm.phone,
            company_key: company.company_key,
            company_name: company.name,
            full_name: dm.full_name,
          });

          newPeople.push({
            person_key: personKey,
            full_name: dm.full_name,
            title: dm.title || null,
            email: normalizeEmail(dm.email),
            phone: normalizePhone(dm.phone),
            linkedin_url: canonicalizeLinkedInUrl(dm.linkedin_url),
            normalized_company_name: normalizeCompanyNameForMatching(
              company.name
            ),
            normalized_company_domain: company.clean_domain,
          });
        }
      }

      // Insert new people
      if (newPeople.length > 0) {
        const peopleResult = await upsertPeople(newPeople);
        stats.decision_makers.added = peopleResult.inserted;

        // Link new decision makers to company
        const companyLinks = peopleResult.records.map((p) => ({
          company_id: companyId,
          person_id: p.id,
          role: "decision_maker" as const,
        }));
        await upsertCompanyPeople(companyLinks);

        // Link to job post if available
        if (jobPostId) {
          const jobLinks = peopleResult.records.map((p) => ({
            job_post_id: jobPostId,
            person_id: p.id,
            role: "decision_maker" as const,
          }));
          await upsertJobPostPeople(jobLinks);
        }
      }
    }

    // Step 4: Update existing contact persons with enriched data
    if (payload.contact_persons.length > 0) {
      for (const cp of payload.contact_persons) {
        const existing = await findExistingPerson({
          linkedin_url: cp.linkedin_url,
          email: cp.email,
          phone: cp.phone,
          company_id: companyId,
          full_name: cp.full_name,
        });

        if (existing) {
          const result = await updatePersonEnrichment(existing.id, {
            title: cp.title,
            email: normalizeEmail(cp.email),
            phone: normalizePhone(cp.phone),
            linkedin_url: canonicalizeLinkedInUrl(cp.linkedin_url),
          });

          if (result.fieldsUpdated.length > 0) {
            stats.contact_persons.updated++;
            stats.contact_persons.fieldsUpdated.push(
              ...result.fieldsUpdated.map((f) => `${cp.full_name}:${f}`)
            );
          }
        }
      }
    }
  } catch (error) {
    stats.errors.push(error instanceof Error ? error.message : String(error));
  }

  return stats;
}

export default async function clayEnrichmentRoutes(
  app: FastifyInstance,
  _opts: FastifyPluginOptions
) {
  // Single enrichment endpoint
  app.post("/clay-enrichment", async (request, reply) => {
    // Optional: Verify auth header
    if (CLAY_ENRICHMENT_SECRET) {
      const authHeader =
        request.headers["x-clay-secret"] || request.headers["authorization"];
      const expectedBearer = `Bearer ${CLAY_ENRICHMENT_SECRET}`;

      if (
        authHeader !== CLAY_ENRICHMENT_SECRET &&
        authHeader !== expectedBearer
      ) {
        return reply.status(401).send({
          error: "Unauthorized",
          message: "Invalid or missing authentication",
        });
      }
    }

    // Validate payload
    const parsed = ClayEnrichmentPayloadSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.status(400).send({
        error: "Bad Request",
        message: parsed.error.message,
        issues: parsed.error.issues,
      });
    }

    // Process enrichment
    const stats = await handleClayEnrichment(parsed.data);

    // Return appropriate status based on results
    if (stats.errors.length > 0 && !stats.company.found) {
      return reply.status(404).send({
        error: "Not Found",
        message: stats.errors[0],
        stats,
      });
    }

    return reply.send({
      status: "ok",
      stats,
    });
  });

  // Batch endpoint for multiple enrichments
  app.post("/clay-enrichment/batch", async (request, reply) => {
    // Auth check
    if (CLAY_ENRICHMENT_SECRET) {
      const authHeader =
        request.headers["x-clay-secret"] || request.headers["authorization"];
      if (
        authHeader !== CLAY_ENRICHMENT_SECRET &&
        authHeader !== `Bearer ${CLAY_ENRICHMENT_SECRET}`
      ) {
        return reply.status(401).send({
          error: "Unauthorized",
          message: "Invalid or missing authentication",
        });
      }
    }

    const batchSchema = z.object({
      items: z.array(ClayEnrichmentPayloadSchema).min(1).max(100),
    });

    const parsed = batchSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.status(400).send({
        error: "Bad Request",
        message: parsed.error.message,
      });
    }

    const results: EnrichmentStats[] = [];
    for (const item of parsed.data.items) {
      const stats = await handleClayEnrichment(item);
      results.push(stats);
    }

    const summary = {
      total: results.length,
      successful: results.filter((r) => r.errors.length === 0).length,
      failed: results.filter((r) => r.errors.length > 0).length,
      companies_enriched: results.filter(
        (r) => r.company.fieldsUpdated.length > 0
      ).length,
      decision_makers_added: results.reduce(
        (sum, r) => sum + r.decision_makers.added,
        0
      ),
      contact_persons_updated: results.reduce(
        (sum, r) => sum + r.contact_persons.updated,
        0
      ),
    };

    return reply.send({
      status: "ok",
      summary,
      results,
    });
  });
}
