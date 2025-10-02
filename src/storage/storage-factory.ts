import { StorageFactory, StorageConfig, StorageMode, StorageBackend } from './interfaces';
import { MemoryStorage } from './memory-storage';
import { SegmentStorage } from './segment-storage';
import { debugLog } from '../debug';

/**
 * Storage factory that creates high-performance segment-based storage
 */
export class DefaultStorageFactory implements StorageFactory {
  private config: StorageConfig;
  private storageInstances = new Map<string, SegmentStorage<any>>();
  private currentDatabaseId: string = 'default';

  constructor(config: StorageConfig) {
    this.config = config;
  }

  // Set the current database context
  setCurrentDatabase(databaseId: string): void {
    this.currentDatabaseId = databaseId;
  }

  createItemStorage<T>(collectionRid?: string): StorageBackend<T> {
    if (this.config.mode === 'memory') {
      return new MemoryStorage<T>();
    }
    
    // Use current database context and collection RID
    const databaseId = this.currentDatabaseId;
    const collectionId = collectionRid || 'default';
    const storageKey = `${databaseId}:${collectionId}`;
    
    if (!this.storageInstances.has(storageKey)) {
      debugLog(`Creating SegmentStorage for key: ${storageKey}`);
      const storage = new SegmentStorage<any>({
        dataDir: this.config.dataDir,
        databaseId,
        collectionId,
        partitionKey: 'default'
      });
      
      // Initialize storage
      storage.initialize().catch(error => {
        console.warn(`Failed to initialize storage for ${storageKey}:`, error);
      });
      
      this.storageInstances.set(storageKey, storage);
      debugLog(`Created storage for database: ${databaseId}, collection: ${collectionId}`);
    }
    
    const storage = this.storageInstances.get(storageKey)!;
    debugLog(`Returning storage for ${storageKey}, has keysSync: ${typeof storage.keysSync === 'function'}`);
    return storage as StorageBackend<T>;
  }
  
  createIndexStorage(collectionRid?: string): StorageBackend<string> {
    // Use separate storage for indexes by appending "_index" to collection ID
    const indexCollectionId = collectionRid ? `${collectionRid}_index` : 'default_index';
    return this.createItemStorage<string>(indexCollectionId);
  }

  getMetrics() {
    return {
      operationsPerSecond: 0,
      averageLatency: 0,
      errorRate: 0,
      memoryUsage: process.memoryUsage().heapUsed
    };
  }
  
  async initialize(): Promise<void> {
    // Initialize all existing storage instances
    for (const storage of this.storageInstances.values()) {
      await storage.initialize();
    }
  }
  
  async close(): Promise<void> {
    // Close all storage instances
    for (const storage of this.storageInstances.values()) {
      await storage.close();
    }
    this.storageInstances.clear();
  }
}

/**
 * Load storage configuration from environment/CLI
 */
export function loadStorageConfig(storageMode: string = 'memory'): StorageConfig {
  return {
    mode: storageMode as StorageMode,
    dataDir: './cosmos-local-data',
    persistence: storageMode === 'file',
    compression: false,
    cacheSize: 10000,
    walEnabled: storageMode === 'file'
  };
}

/**
 * Create storage factory based on configuration
 */
export function createStorageFactory(config: StorageConfig): StorageFactory {
  return new DefaultStorageFactory(config);
}
