import { FastifyReply, FastifyRequest } from "fastify";
import { getUserHasAdminAccessToSite } from "../../lib/auth-utils.js";
import { importQuotaManager } from "../../services/import/importQuotaManager.js";
import { DateTime } from "luxon";
import { z } from "zod";
import { db } from "../../db/postgres/postgres.js";
import { sites, importStatus } from "../../db/postgres/schema.js";
import { eq } from "drizzle-orm";

const createSiteImportRequestSchema = z
  .object({
    params: z.object({
      site: z.string().min(1),
    }),
  })
  .strict();

type CreateSiteImportRequest = {
  Params: z.infer<typeof createSiteImportRequestSchema.shape.params>;
};

export async function createSiteImport(request: FastifyRequest<CreateSiteImportRequest>, reply: FastifyReply) {
  try {
    const parsed = createSiteImportRequestSchema.safeParse({
      params: request.params,
    });

    if (!parsed.success) {
      return reply.status(400).send({ error: "Validation error" });
    }

    const { site } = parsed.data.params;
    const siteId = Number(site);

    const userHasAccess = await getUserHasAdminAccessToSite(request, site);
    if (!userHasAccess) {
      return reply.status(403).send({ error: "Forbidden" });
    }

    // Get site's organization
    const [siteRecord] = await db
      .select({ organizationId: sites.organizationId })
      .from(sites)
      .where(eq(sites.siteId, siteId))
      .limit(1);

    if (!siteRecord) {
      return reply.status(404).send({ error: "Site not found" });
    }

    const organizationId = siteRecord.organizationId;

    // Check concurrent import limit using singleton
    if (!importQuotaManager.canStartImport(organizationId)) {
      return reply.status(429).send({ error: "Only 1 concurrent import allowed per organization" });
    }

    // Get quota information from cached tracker
    const quotaTracker = await importQuotaManager.getTracker(organizationId);
    const summary = quotaTracker.getSummary();

    // Calculate the earliest and latest allowed dates
    const oldestAllowedDate = DateTime.fromFormat(summary.oldestAllowedMonth + "01", "yyyyMMdd", { zone: "utc" });
    const earliestAllowedDate = oldestAllowedDate.toFormat("yyyy-MM-dd");
    const latestAllowedDate = DateTime.utc().toFormat("yyyy-MM-dd");

    // Create import record
    const [importRecord] = await db
      .insert(importStatus)
      .values({
        siteId,
        organizationId,
      })
      .returning({ importId: importStatus.importId });

    // Register import with quota manager
    importQuotaManager.registerImport(organizationId, importRecord.importId);

    return reply.send({
      data: {
        importId: importRecord.importId,
        allowedDateRange: {
          earliestAllowedDate,
          latestAllowedDate,
        },
      },
    });
  } catch (error) {
    console.error("Error creating import:", error);
    return reply.status(500).send({ error: "Internal server error" });
  }
}
