import { FastifyReply, FastifyRequest } from "fastify";
import { z } from "zod";
import { getUserHasAdminAccessToSite } from "../../lib/auth-utils.js";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { updateImportProgress, completeImport, getImportById } from "../../services/import/importStatusManager.js";
import { UmamiImportMapper } from "../../services/import/mappings/umami.js";
import { importQuotaManager } from "../../services/import/importQuotaManager.js";
import { db } from "../../db/postgres/postgres.js";
import { sites, importStatus } from "../../db/postgres/schema.js";
import { eq } from "drizzle-orm";

const batchImportRequestSchema = z
  .object({
    params: z.object({
      site: z.string().min(1),
      importId: z.string().uuid(),
    }),
    body: z.object({
      events: z.array(UmamiImportMapper.umamiEventKeyOnlySchema).min(1).max(10000),
      isLastBatch: z.boolean().optional(),
    }),
  })
  .strict();

type BatchImportRequest = {
  Params: z.infer<typeof batchImportRequestSchema.shape.params>;
  Body: z.infer<typeof batchImportRequestSchema.shape.body>;
};

export async function batchImportEvents(request: FastifyRequest<BatchImportRequest>, reply: FastifyReply) {
  try {
    const parsed = batchImportRequestSchema.safeParse({
      params: request.params,
      body: request.body,
    });

    if (!parsed.success) {
      return reply.status(400).send({ error: "Validation error" });
    }

    const { site, importId } = parsed.data.params;
    const { events, isLastBatch } = parsed.data.body;
    const siteId = Number(site);

    const userHasAccess = await getUserHasAdminAccessToSite(request, site);
    if (!userHasAccess) {
      return reply.status(403).send({ error: "Forbidden" });
    }

    // Verify import exists
    const importRecord = await getImportById(importId);
    if (!importRecord) {
      return reply.status(404).send({ error: "Import not found" });
    }

    if (importRecord.siteId !== siteId) {
      return reply.status(400).send({ error: "Import does not belong to this site" });
    }

    // Check if import is already completed
    if (importRecord.completedAt) {
      return reply.status(400).send({ error: "Import already completed" });
    }

    // Auto-detect platform if not set (first batch)
    let detectedPlatform = importRecord.platform;
    if (!detectedPlatform) {
      const firstEvent = events[0];

      if (UmamiImportMapper.umamiEventKeyOnlySchema.safeParse(firstEvent).success) {
        detectedPlatform = "umami";
      } else {
        return reply.status(400).send({ error: "Unable to detect platform from event structure" });
      }

      await db.update(importStatus).set({ platform: detectedPlatform }).where(eq(importStatus.importId, importId));
    }

    const [siteRecord] = await db
      .select({ organizationId: sites.organizationId })
      .from(sites)
      .where(eq(sites.siteId, siteId))
      .limit(1);

    if (!siteRecord) {
      return reply.status(404).send({ error: "Site not found" });
    }

    try {
      // Get cached quota tracker from singleton (much faster!)
      const quotaTracker = await importQuotaManager.getTracker(siteRecord.organizationId);

      // Transform events (validate and convert to internal format)
      const transformedEvents = UmamiImportMapper.transform(events, site, importId);
      const invalidEventCount = events.length - transformedEvents.length;

      // Filter events through quota checker
      const eventsWithinQuota = [];
      let skippedDueToQuota = 0;

      for (const event of transformedEvents) {
        if (quotaTracker.canImportEvent(event.timestamp)) {
          eventsWithinQuota.push(event);
        } else {
          skippedDueToQuota++;
        }
      }

      // Insert events to ClickHouse (even if 0 events, we still need to track progress)
      if (eventsWithinQuota.length > 0) {
        await clickhouse.insert({
          table: "events",
          values: eventsWithinQuota,
          format: "JSONEachRow",
        });
      }

      // Update import progress
      await updateImportProgress(importId, eventsWithinQuota.length, skippedDueToQuota, invalidEventCount);

      // If this is the last batch, mark import as completed
      if (isLastBatch) {
        await completeImport(importId);
        importQuotaManager.completeImport(siteRecord.organizationId, importId);
      }

      // Return counts to client
      return reply.send({
        imported: eventsWithinQuota.length,
        skipped: skippedDueToQuota,
        invalid: invalidEventCount,
      });
    } catch (insertError) {
      const errorMessage = insertError instanceof Error ? insertError.message : "Unknown error";
      console.error("Failed to insert events:", errorMessage);

      return reply.status(500).send({
        error: `Failed to insert events: ${errorMessage}`,
      });
    }
  } catch (error) {
    console.error("Error importing events", error);
    return reply.status(500).send({ error: "Internal server error" });
  }
}
