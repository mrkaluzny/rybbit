import { ImportQuotaTracker } from "./importQuotaChecker.js";
import { IS_CLOUD } from "../../lib/const.js";

interface CachedTracker {
  tracker: ImportQuotaTracker;
  lastAccessed: number;
}

interface ActiveImport {
  importId: string;
  startedAt: number;
}

/**
 * Singleton service that manages quota trackers and rate limiting for imports
 * Caches quota trackers per organization to avoid expensive ClickHouse queries
 */
class ImportQuotaManager {
  // Cache of quota trackers by organizationId
  private trackers: Map<string, CachedTracker> = new Map();

  // Active imports tracking for rate limiting
  private activeImports: Map<string, Set<ActiveImport>> = new Map();

  // Configuration
  private readonly CONCURRENT_IMPORT_LIMIT = 1; // Max concurrent imports per organization
  private readonly TRACKER_TTL_MS = 30 * 60 * 1000; // 30 minutes
  private readonly IMPORT_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 hours

  /**
   * Get a quota tracker for the given organization
   * Returns cached tracker if available, otherwise creates a new one
   */
  async getTracker(organizationId: string): Promise<ImportQuotaTracker> {
    const now = Date.now();
    const cached = this.trackers.get(organizationId);

    // Return cached tracker if it's still valid
    if (cached && now - cached.lastAccessed < this.TRACKER_TTL_MS) {
      cached.lastAccessed = now;
      return cached.tracker;
    }

    // Create new tracker
    const tracker = await ImportQuotaTracker.create(organizationId);
    this.trackers.set(organizationId, {
      tracker,
      lastAccessed: now,
    });

    return tracker;
  }

  /**
   * Check if an organization can start a new import
   * Based on concurrent import limit
   */
  canStartImport(organizationId: string): boolean {
    if (!IS_CLOUD) {
      return true; // No limits for self-hosted
    }

    const activeSet = this.activeImports.get(organizationId);
    if (!activeSet) {
      return true; // No active imports
    }

    // Clean up abandoned imports before checking
    this.cleanupAbandonedImports(organizationId);

    return activeSet.size < this.CONCURRENT_IMPORT_LIMIT;
  }

  /**
   * Register a new import as active
   */
  registerImport(organizationId: string, importId: string): void {
    if (!IS_CLOUD) {
      return; // No tracking for self-hosted
    }

    let activeSet = this.activeImports.get(organizationId);
    if (!activeSet) {
      activeSet = new Set();
      this.activeImports.set(organizationId, activeSet);
    }

    activeSet.add({
      importId,
      startedAt: Date.now(),
    });
  }

  /**
   * Mark an import as completed (remove from active tracking)
   */
  completeImport(organizationId: string, importId: string): void {
    const activeSet = this.activeImports.get(organizationId);
    if (!activeSet) {
      return;
    }

    // Find and remove the import
    for (const activeImport of activeSet) {
      if (activeImport.importId === importId) {
        activeSet.delete(activeImport);
        break;
      }
    }

    // Clean up empty sets
    if (activeSet.size === 0) {
      this.activeImports.delete(organizationId);
    }
  }

  /**
   * Remove abandoned imports (no activity for > IMPORT_TIMEOUT_MS)
   */
  private cleanupAbandonedImports(organizationId: string): void {
    const activeSet = this.activeImports.get(organizationId);
    if (!activeSet) {
      return;
    }

    const now = Date.now();
    const toRemove: ActiveImport[] = [];

    for (const activeImport of activeSet) {
      if (now - activeImport.startedAt > this.IMPORT_TIMEOUT_MS) {
        toRemove.push(activeImport);
      }
    }

    for (const importToRemove of toRemove) {
      activeSet.delete(importToRemove);
    }

    // Clean up empty sets
    if (activeSet.size === 0) {
      this.activeImports.delete(organizationId);
    }
  }

  /**
   * Periodic cleanup of stale trackers and abandoned imports
   * Called automatically by setInterval
   */
  cleanup(): void {
    const now = Date.now();

    // Clean up stale trackers
    for (const [orgId, cached] of this.trackers.entries()) {
      if (now - cached.lastAccessed > this.TRACKER_TTL_MS) {
        this.trackers.delete(orgId);
      }
    }

    // Clean up abandoned imports across all organizations
    for (const orgId of this.activeImports.keys()) {
      this.cleanupAbandonedImports(orgId);
    }
  }

  /**
   * Get debug information about current state
   */
  getDebugInfo(): {
    cachedTrackers: number;
    activeImports: Record<string, number>;
  } {
    const activeImports: Record<string, number> = {};
    for (const [orgId, activeSet] of this.activeImports.entries()) {
      activeImports[orgId] = activeSet.size;
    }

    return {
      cachedTrackers: this.trackers.size,
      activeImports,
    };
  }
}

// Export singleton instance
export const importQuotaManager = new ImportQuotaManager();

// Set up automatic cleanup every 15 minutes
if (IS_CLOUD) {
  setInterval(
    () => {
      importQuotaManager.cleanup();
    },
    15 * 60 * 1000
  );
}
