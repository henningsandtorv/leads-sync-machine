import { FastifyInstance, FastifyPluginOptions } from 'fastify';
import { z } from 'zod';
import { normalizeDomainHost } from '../lib/normalize';
import { upsertCompanies, upsertPeople, upsertCompanyPeople, CompanyRecord, PersonRecord } from '../lib/db';

const companySchema = z.object({
  company_key: z.string().min(1),
  name: z.string().min(1),
  domain: z.string().optional(),
  orgnr: z.string().optional(),
  proff_url: z.string().url().optional(),
  industry: z.string().optional(),
  company_size: z.string().optional(),
  location: z.string().optional(),
});

const personSchema = z.object({
  person_key: z.string().min(1),
  full_name: z.string().min(1),
  title: z.string().optional(),
  email: z.string().optional(),
  phone: z.string().optional(),
  linkedin_url: z.string().optional(),
});

const linkSchema = z.object({
  company_key: z.string().min(1),
  person_key: z.string().min(1),
  role: z.enum(['contact_person', 'decision_maker', 'recruiter', 'other']),
});

const bodySchema = z.object({
  companies: z.array(companySchema).default([]),
  people: z.array(personSchema).default([]),
  links: z.array(linkSchema).default([]),
});

export default async function importRoutes(app: FastifyInstance, _opts: FastifyPluginOptions) {
  app.post('/normalized', async (request, reply) => {
    const parsed = bodySchema.safeParse(request.body);
    if (!parsed.success) {
      reply.status(400).send({ error: 'Bad Request', message: parsed.error.message });
      return;
    }

    const companies: CompanyRecord[] = parsed.data.companies.map((c) => ({
      ...c,
      domain: normalizeDomainHost(c.domain),
    }));

    const people: PersonRecord[] = parsed.data.people.map((p) => ({
      ...p,
    }));

    // Upsert companies and people first to get their IDs
    const [companyResult, peopleResult] = await Promise.all([
      upsertCompanies(companies),
      upsertPeople(people),
    ]);

    // Build a map of keys to IDs
    const companyIdMap = new Map<string, string>();
    for (const company of companyResult.records) {
      if (company.company_key && company.id) {
        companyIdMap.set(company.company_key, company.id);
      }
    }

    const personIdMap = new Map<string, string>();
    for (const person of peopleResult.records) {
      if (person.person_key && person.id) {
        personIdMap.set(person.person_key, person.id);
      }
    }

    // Convert links to use UUIDs
    const links = parsed.data.links
      .map((l) => {
        const companyId = companyIdMap.get(l.company_key);
        const personId = personIdMap.get(l.person_key);
        if (!companyId || !personId) return null;
        return {
          company_id: companyId,
          person_id: personId,
          role: l.role,
        };
      })
      .filter((link): link is NonNullable<typeof link> => link !== null);

    const linkResult = await upsertCompanyPeople(links);

    reply.send({
      companies: companyResult,
      people: peopleResult,
      company_people: linkResult,
    });
  });
}
