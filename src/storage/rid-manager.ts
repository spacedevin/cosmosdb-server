import { debugLog } from '../debug';

/**
 * Centralized RID (Resource ID) generator for CosmosDB compatibility
 * This ensures unique RIDs across all collections and databases
 */
export class RidManager {
  private static instance: RidManager;
  private ridCounters = new Map<string, number>();
  private globalCounter = 1;
  private initialized = new Set<string>(); // Track which namespaces have been initialized

  private constructor() {}

  static getInstance(): RidManager {
    if (!RidManager.instance) {
      RidManager.instance = new RidManager();
    }
    return RidManager.instance;
  }

  /**
   * Initialize RID counter from existing storage (only once per namespace)
   */
  initializeFromStorage(storage: any, namespace: string): void {
    // Skip if already initialized for this namespace
    if (this.initialized.has(namespace)) {
      debugLog(`RID Manager: Namespace ${namespace} already initialized, skipping`);
      return;
    }

    let maxRid = 0;
    
    try {
      const keys = storage.keysSync();
      
      for (const key of keys) {
        if (key.includes(':')) {
          const item = storage.getSync(key);
          if (item && item._rid) {
            const ridNumber = this.extractRidNumber(item._rid);
            if (ridNumber > maxRid) {
              maxRid = ridNumber;
            }
          }
        }
      }
      
      // Set counters only if we haven't seen this namespace before
      const newCounter = maxRid + 1;
      this.ridCounters.set(namespace, newCounter);
      this.globalCounter = Math.max(this.globalCounter, newCounter);
      this.initialized.add(namespace);
      
      debugLog(`RID Manager: Initialized ${namespace} counter to ${newCounter}, global counter to ${this.globalCounter}`);
    } catch (error) {
      console.warn(`RID Manager: Failed to initialize from storage:`, error);
      this.ridCounters.set(namespace, this.globalCounter);
      this.initialized.add(namespace);
    }
  }

  /**
   * Extract numeric part from base64 RID
   */
  private extractRidNumber(rid: string): number {
    try {
      const decoded = Buffer.from(rid, 'base64');
      const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
      if (view.byteLength >= 8) {
        return view.getUint32(4, false); // Big-endian, second 32 bits
      }
      return 0;
    } catch {
      return 0;
    }
  }

  /**
   * Generate a unique RID for documents
   */
  generateDocumentRid(namespace: string = 'global'): string {
    // Get or initialize counter for namespace
    if (!this.ridCounters.has(namespace)) {
      this.ridCounters.set(namespace, this.globalCounter);
      this.initialized.add(namespace);
    }
    
    const ridNumber = this.ridCounters.get(namespace)!;
    const nextCounter = ridNumber + 1;
    this.ridCounters.set(namespace, nextCounter);
    this.globalCounter = Math.max(this.globalCounter, nextCounter);
    
    // Create proper CosmosDB-style document RID
    const buffer = Buffer.alloc(8);
    buffer.writeUInt32BE(0x80000004, 0); // Fixed prefix for document RIDs
    buffer.writeUInt32BE(ridNumber, 4);   // Unique incremental number
    
    const rid = buffer.toString('base64');
    debugLog(`RID Manager: Generated document RID ${rid} (${ridNumber}) for ${namespace}, next will be ${nextCounter}`);
    return rid;
  }

  /**
   * Generate a unique RID for collections
   */
  generateCollectionRid(namespace: string = 'global'): string {
    if (!this.ridCounters.has(namespace)) {
      this.ridCounters.set(namespace, this.globalCounter);
    }
    
    const ridNumber = this.ridCounters.get(namespace)!;
    this.ridCounters.set(namespace, ridNumber + 1);
    this.globalCounter = Math.max(this.globalCounter, ridNumber + 1);
    
    // Create collection RID with different prefix
    const buffer = Buffer.alloc(8);
    buffer.writeUInt32BE(0x80000002, 0); // Fixed prefix for collection RIDs
    buffer.writeUInt32BE(ridNumber, 4);   // Unique incremental number
    
    const rid = buffer.toString('base64');
    debugLog(`RID Manager: Generated collection RID ${rid} (${ridNumber}) for ${namespace}`);
    return rid;
  }

  /**
   * Generate a unique RID for databases
   */
  generateDatabaseRid(namespace: string = 'global'): string {
    if (!this.ridCounters.has(namespace)) {
      this.ridCounters.set(namespace, this.globalCounter);
    }
    
    const ridNumber = this.ridCounters.get(namespace)!;
    this.ridCounters.set(namespace, ridNumber + 1);
    this.globalCounter = Math.max(this.globalCounter, ridNumber + 1);
    
    // Create database RID with different prefix
    const buffer = Buffer.alloc(8);
    buffer.writeUInt32BE(0x80000001, 0); // Fixed prefix for database RIDs
    buffer.writeUInt32BE(ridNumber, 4);   // Unique incremental number
    
    const rid = buffer.toString('base64');
    debugLog(`RID Manager: Generated database RID ${rid} (${ridNumber}) for ${namespace}`);
    return rid;
  }

  /**
   * Get current counter for a namespace
   */
  getCurrentCounter(namespace: string): number {
    return this.ridCounters.get(namespace) || 1;
  }

  /**
   * Reset all counters (for testing)
   */
  reset(): void {
    this.ridCounters.clear();
    this.initialized.clear();
    this.globalCounter = 1;
    debugLog('RID Manager: Reset all counters');
  }
}