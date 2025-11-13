import { FastifyReply, FastifyRequest } from "fastify";
import { z } from "zod";
import { getUserHasAdminAccessToSite } from "../../lib/auth-utils.js";
import { getImportById, deleteImport } from "../../services/import/importStatusManager.js";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { importQuotaManager } from "../../services/import/importQuotaManager.js";
import { db } from "../../db/postgres/postgres.js";
import { sites } from "../../db/postgres/schema.js";
import { eq } from "drizzle-orm";

const deleteImportRequestSchema = z
  .object({
    params: z.object({
      site: z.string().min(1),
      importId: z.string().uuid(),
    }),
  })
  .strict();

type DeleteImportRequest = {
  Params: z.infer<typeof deleteImportRequestSchema.shape.params>;
};

export async function deleteSiteImport(request: FastifyRequest<DeleteImportRequest>, reply: FastifyReply) {
  try {
    const parsed = deleteImportRequestSchema.safeParse({
      params: request.params,
    });

    if (!parsed.success) {
      return reply.status(400).send({ error: "Validation error" });
    }

    const { site, importId } = parsed.data.params;

    const userHasAccess = await getUserHasAdminAccessToSite(request, site);
    if (!userHasAccess) {
      return reply.status(403).send({ error: "Forbidden" });
    }

    const importRecord = await getImportById(importId);
    if (!importRecord) {
      return reply.status(404).send({ error: "Import not found" });
    }

    if (importRecord.siteId !== Number(site)) {
      return reply.status(403).send({ error: "Import does not belong to this site" });
    }

    // Cannot delete imports that are still in progress
    if (importRecord.completedAt === null) {
      return reply.status(400).send({ error: "Cannot delete active import" });
    }

    const siteId = Number(site);

    // Get organization ID for quota manager notification
    const [siteRecord] = await db
      .select({ organizationId: sites.organizationId })
      .from(sites)
      .where(eq(sites.siteId, siteId))
      .limit(1);

    try {
      await clickhouse.command({
        query: `DELETE FROM events WHERE import_id = {importId:String} AND site_id = {siteId:UInt16}`,
        query_params: {
          importId: importId,
          siteId: siteId,
        },
      });
      console.log(`Deleted events for import ${importId} from ClickHouse`);
    } catch (chError) {
      console.error(`Failed to delete ClickHouse events for ${importId}:`, chError);
      return reply.status(500).send({
        error: "Failed to delete imported events",
      });
    }

    try {
      await deleteImport(importId);
    } catch (dbError) {
      console.error(`Failed to delete import record ${importId}:`, dbError);
      return reply.status(500).send({
        error: "Failed to delete import record",
      });
    }

    // Notify quota manager that import is no longer active
    if (siteRecord) {
      importQuotaManager.completeImport(siteRecord.organizationId, importId);
    }

    return reply.send({
      data: {
        message: "Import deleted successfully",
      },
    });
  } catch (error) {
    console.error("Error deleting import:", error);
    return reply.status(500).send({ error: "Internal server error" });
  }
}
