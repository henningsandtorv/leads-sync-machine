import { FastifyInstance, FastifyPluginOptions } from "fastify";
import {
  getRecentJobPostIds,
  getJobPostWithDecisionMakers,
} from "../lib/db";
import {
  sendBatchToClayWebhook,
  isClayWebhookEnabled,
  ClayJobPostPayload,
} from "../lib/clay";

export default async function cronRoutes(
  app: FastifyInstance,
  _opts: FastifyPluginOptions
) {
  /**
   * Batch sync recent job posts to Clay
   * Designed to be called by Vercel cron or manually
   */
  app.get("/clay-sync", async (request, reply) => {
    if (!isClayWebhookEnabled()) {
      return reply.send({
        status: "skipped",
        message: "CLAY_WEBHOOK_URL not configured",
      });
    }

    // Get job posts from last 24 hours
    const hoursAgo = 24;
    const jobPostIds = await getRecentJobPostIds(hoursAgo);

    if (jobPostIds.length === 0) {
      return reply.send({
        status: "ok",
        message: "No recent job posts to sync",
        synced: 0,
      });
    }

    // Build Clay payloads for each job post
    const payloads: ClayJobPostPayload[] = [];
    for (const jobPostId of jobPostIds) {
      const enrichedJobPost = await getJobPostWithDecisionMakers(jobPostId);
      if (!enrichedJobPost) continue;

      payloads.push({
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
        contact_persons: enrichedJobPost.contact_persons.map((cp) => ({
          full_name: cp.full_name,
          title: cp.title,
          email: cp.email,
          phone: cp.phone,
          linkedin_url: cp.linkedin_url,
        })),
      });
    }

    // Send to Clay with rate limiting (100ms between requests)
    const result = await sendBatchToClayWebhook(payloads, 100);

    return reply.send({
      status: "ok",
      total_job_posts: jobPostIds.length,
      payloads_built: payloads.length,
      synced: result.success,
      failed: result.failed,
    });
  });
}
