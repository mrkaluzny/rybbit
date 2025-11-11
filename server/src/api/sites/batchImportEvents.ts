import { FastifyReply, FastifyRequest } from "fastify";
import { z } from "zod";
import { getUserHasAdminAccessToSite } from "../../lib/auth-utils.js";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { updateImportProgress, updateImportStatus, getImportById } from "../../services/import/importStatusManager.js";
import { UmamiImportMapper, type UmamiEvent } from "../../services/import/mappings/umami.js";
import { ImportQuotaTracker } from "../../services/import/importQuotaChecker.js";
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

    // Verify import exists and is in valid state
    const importRecord = await getImportById(importId);
    if (!importRecord) {
      return reply.status(404).send({ error: "Import not found" });
    }

    if (importRecord.siteId !== siteId) {
      return reply.status(400).send({ error: "Import does not belong to this site" });
    }

    if (importRecord.status === "completed") {
      return reply.status(400).send({ error: "Import already completed" });
    }

    if (importRecord.status === "failed") {
      return reply.status(400).send({ error: "Import has failed" });
    }

    if (importRecord.status === "pending") {
      await updateImportStatus(importId, "processing");
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
      const quotaTracker = await ImportQuotaTracker.create(siteRecord.organizationId);

      const transformedEvents = UmamiImportMapper.transform(events, site, importId);
      const invalidEventCount = events.length - transformedEvents.length;

      const eventsWithinQuota = [];
      let skippedDueToQuota = 0;

      for (const event of transformedEvents) {
        if (quotaTracker.canImportEvent(event.timestamp)) {
          eventsWithinQuota.push(event);
        } else {
          skippedDueToQuota++;
        }
      }

      if (eventsWithinQuota.length === 0) {
        const quotaSummary = quotaTracker.getSummary();
        let quotaMessage =
          `All ${events.length} events exceeded monthly quotas or fell outside the ${quotaSummary.totalMonthsInWindow}-month historical window. ` +
          `${quotaSummary.monthsAtCapacity} of ${quotaSummary.totalMonthsInWindow} months are at full capacity.`;

        if (invalidEventCount > 0) {
          quotaMessage += ` ${invalidEventCount} events were invalid.`;
        }

        if (isLastBatch) {
          await updateImportStatus(importId, "completed", quotaMessage);
        }

        return reply.send({
          quotaExceeded: true,
          message: quotaMessage,
        });
      }

      await clickhouse.insert({
        table: "events",
        values: eventsWithinQuota,
        format: "JSONEachRow",
      });

      await updateImportProgress(importId, eventsWithinQuota.length);

      if (isLastBatch) {
        let finalMessage: string | undefined = undefined;
        const messages: string[] = [];

        if (skippedDueToQuota > 0) {
          messages.push(`${skippedDueToQuota} events were skipped due to quota limits`);
        }

        if (invalidEventCount > 0) {
          messages.push(`${invalidEventCount} events were invalid`);
        }

        if (messages.length > 0) {
          finalMessage = `Import completed. ${messages.join("; ")}.`;
        }

        await updateImportStatus(importId, "completed", finalMessage);
      }

      return reply.send({});
    } catch (insertError) {
      const errorMessage = insertError instanceof Error ? insertError.message : "Unknown error";
      await updateImportStatus(importId, "failed", `Failed to insert events: ${errorMessage}`);

      return reply.status(500).send({
        error: `Failed to insert events: ${errorMessage}`,
      });
    }
  } catch (error) {
    console.error("Error importing events", error);
    return reply.status(500).send({ error: "Internal server error" });
  }
}
