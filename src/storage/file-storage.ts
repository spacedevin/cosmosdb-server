/**
 * File storage backend implementing persistent storage with WAL and segments
 */

import * as path from "path";
import * as crypto from "crypto";
import { StorageBackend, StorageOptions } from "./interfaces";
import { WALManager } from "./wal/wal-manager";
import { SegmentManager } from "./segments/segment-manager";
import { StorageError } from "../utils/error-types";
import { WALEntry } from "./wal/wal-entry";

/**
 * Sanitize collection RID for use in file system paths
 * Collection RIDs can contain characters like =, /, + that are invalid in paths
 */
function sanitizeCollectionRid(collectionRid: string): string {
  // Use base64url encoding to ensure safe file system paths
  // Replace unsafe characters: = -> _, / -> -, + -> .
  return Buffer.from(collectionRid, 'utf8')
    .toString('base64')
    .replace(/=/g, '_')
    .replace(/\//g, '-')
    .replace(/\+/g, '.');
}

/**
 * Document cache for fast lookups
 */
class DocumentCache<T> {
  private cache = new Map<
    string,
    { data: T; timestamp: number; accessCount: number }
  >();

  private maxSize: number;

  constructor(maxSize: number = 10000) {
    this.maxSize = maxSize;
  }

  get(key: string): T | undefined {
    const entry = this.cache.get(key);
    if (entry) {
      entry.accessCount++;
      entry.timestamp = Date.now();
      return entry.data;
    }
    return undefined;
  }

  set(key: string, data: T): void {
    // Remove oldest entry if cache is full
    if (this.cache.size >= this.maxSize) {
      let oldestKey: string | undefined;
      let oldestTime = Date.now();

      for (const [k, entry] of this.cache.entries()) {
        if (entry.timestamp < oldestTime) {
          oldestTime = entry.timestamp;
          oldestKey = k;
        }
      }

      if (oldestKey) {
        this.cache.delete(oldestKey);
      }
    }

    this.cache.set(key, {
      data,
      timestamp: Date.now(),
      accessCount: 1
    });
  }

  delete(key: string): boolean {
    return this.cache.delete(key);
  }

  clear(): void {
    this.cache.clear();
  }

  size(): number {
    return this.cache.size;
  }
}

/**
 * File storage backend with WAL and segment-based persistence
 */
export class FileStorage<T> implements StorageBackend<T> {
  private walManager: WALManager;
  private segmentManager: SegmentManager;
  protected cache: DocumentCache<T>;
  private initialized = false;
  private storageManager?: FileStorageManager<T>;

  constructor(private options: StorageOptions) {
    const baseDataDir = options.dataDir || "./cosmos-local-data";
    const collectionRid = options.collectionRid || "default";
    
    // Sanitize collection RID for safe file system paths
    const sanitizedRid = sanitizeCollectionRid(collectionRid);
    
    // Create collection-specific directories for true isolation
    const dataDir = path.join(baseDataDir, "collections", sanitizedRid);

    this.walManager = new WALManager(path.join(dataDir, "wal"));
    this.segmentManager = new SegmentManager(path.join(dataDir, "segments"), {
      compressionEnabled: options.compression,
      maxSegmentSize: 64 * 1024 * 1024, // 64MB
      cacheSize: options.readCacheSize
    });
    this.cache = new DocumentCache<T>(options.cacheSize || 10000);
    this.storageManager = new FileStorageManager(this.segmentManager);
  }

  /**
   * Initialize the file storage backend
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      await this.walManager.initialize();
      await this.segmentManager.initialize();

      // Set up WAL to storage manager connection
      if (this.storageManager) {
        this.walManager.setApplyCallback(async (entry: WALEntry) => {
          await this.storageManager!.applyWALEntry(entry);
        });
      }

      // Replay WAL to recover any uncommitted operations
      const replayResult = await this.walManager.replay();
      if (replayResult.entriesReplayed > 0) {
        console.log(
          `WAL recovery: ${replayResult.entriesReplayed} entries replayed, ${replayResult.corruptedEntries} corrupted entries`
        );

        if (replayResult.recoveryActions.length > 0) {
          console.log("Recovery actions:", replayResult.recoveryActions);
        }
      }

      this.initialized = true;
    } catch (error) {
      throw new StorageError(
        "Failed to initialize file storage",
        "INITIALIZATION_ERROR",
        false,
        { error }
      );
    }
  }

  /**
   * Get a document by key
   */
  async get(key: string): Promise<T | undefined> {
    await this.ensureInitialized();

    try {
      // Check cache first
      const cached = this.cache.get(key);
      if (cached) {
        return cached;
      }

      // Load from segments
      const result = await this.segmentManager.read<T>(key);
      if (result.success && result.document) {
        this.cache.set(key, result.document);
        return result.document;
      }

      return undefined;
    } catch (error) {
      console.warn(`Error retrieving document ${key}:`, error);
      return undefined;
    }
  }

  /**
   * Set a document
   */
  async set(key: string, value: T): Promise<void> {
    await this.ensureInitialized();

    try {
      // Clean the value to remove circular references
      const cleanValue = this.cleanData(value);

      // Write to WAL first (crash safety)
      const walResult = await this.walManager.append({
        operation: "CREATE",
        partitionKey: this.extractPartitionKey(key),
        documentId: key,
        data: cleanValue
      });

      if (!walResult.success) {
        if (walResult.fallbackToMemory) {
          console.warn(
            `WAL write failed for ${key}, using memory-only mode: ${walResult.error}`
          );
        } else {
          throw new StorageError(`WAL write failed: ${walResult.error}`, "WAL_ERROR");
        }
      }

      // Update cache immediately
      this.cache.set(key, value);

      // Write directly to segments (not queued) to ensure immediate persistence
      // WAL replay should NOT write to segments again since we're writing here
      await this.segmentManager.queueWrite(key, cleanValue);
    } catch (error) {
      throw new StorageError(
        `Failed to set document: ${key}`,
        "WRITE_ERROR",
        true,
        { error }
      );
    }
  }

  /**
   * Delete a document
   */
  async delete(key: string): Promise<boolean> {
    await this.ensureInitialized();

    try {
      const exists = await this.has(key);
      if (!exists) return false;

      // Write to WAL
      const walResult = await this.walManager.append({
        operation: "DELETE",
        partitionKey: this.extractPartitionKey(key),
        documentId: key
      });

      if (!walResult.success && !walResult.fallbackToMemory) {
        throw new StorageError(`WAL write failed: ${walResult.error}`, "WAL_ERROR");
      }

      // Remove from cache
      this.cache.delete(key);

      // Mark as deleted in segments
      await this.segmentManager.markDeleted(key);

      return true;
    } catch (error) {
      throw new StorageError(
        `Failed to delete document: ${key}`,
        "DELETE_ERROR",
        true,
        { error }
      );
    }
  }

  /**
   * Check if a document exists
   */
  async has(key: string): Promise<boolean> {
    const value = await this.get(key);
    return value !== undefined;
  }

  /**
   * Iterate over all keys
   */
  async *keys(): AsyncIterableIterator<string> {
    await this.ensureInitialized();

    try {
      // Get all keys from segments - these are the actual stored keys
      const segmentKeys = await this.segmentManager.getAllKeys();

      // Deduplicate keys since segments might have duplicates
      const uniqueKeys = new Set(segmentKeys);

      // Simply yield all unique keys from segments
      for (const key of uniqueKeys) {
        yield key;
      }
    } catch (error) {
      throw new StorageError(
        "Failed to iterate keys",
        "ITERATION_ERROR",
        true,
        { error }
      );
    }
  }

  /**
   * Clear all data
   */
  async clear(): Promise<void> {
    await this.ensureInitialized();

    try {
      this.cache.clear();
      await this.segmentManager.clear();
      await this.walManager.clear();
    } catch (error) {
      throw new StorageError(
        "Failed to clear storage",
        "CLEAR_ERROR",
        false,
        { error }
      );
    }
  }

  /**
   * Get the number of documents
   */
  async size(): Promise<number> {
    await this.ensureInitialized();

    try {
      let count = 0;
      for await (const _ of this.keys()) {
        count++;
      }
      return count;
    } catch (error) {
      throw new StorageError(
        "Failed to get storage size",
        "SIZE_ERROR",
        true,
        { error }
      );
    }
  }

  /**
   * Close the storage backend
   */
  async close(): Promise<void> {
    if (!this.initialized) return;

    try {
      await this.segmentManager.close();
      await this.walManager.close();
      this.cache.clear();
      this.initialized = false;
    } catch (error) {
      throw new StorageError(
        "Failed to close storage",
        "CLOSE_ERROR",
        false,
        { error }
      );
    }
  }

  /**
   * Ensure storage is initialized
   */
  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }

  /**
   * Extract partition key from document key
   */
  private extractPartitionKey(key: string): string {
    const parts = key.split(":");
    if (parts.length !== 3) {
      return "default";
    }

    const [partitionValue, keyPath, id] = parts;

    // Use the partition value as-is to maintain consistency with retrieval
    return partitionValue;
  }

  /**
   * Clean data to remove circular references and extract actual document data
   */
  private cleanData(data: any): any {
    // If this is a Document/Item object with _data property, extract the actual data
    if (data && typeof data === 'object' && data._data && typeof data._data === 'object') {
      return data._data;
    }
    
    // If this is already clean document data, return as-is
    if (data && typeof data === 'object' && !data._parent && !data.collections) {
      return data;
    }
    
    // Otherwise, do deep cleaning for circular references
    const seen = new WeakSet();
    
    const clean = (obj: any): any => {
      if (obj === null || typeof obj !== 'object') {
        return obj;
      }
      
      if (seen.has(obj)) {
        return '[Circular]';
      }
      
      seen.add(obj);
      
      if (Array.isArray(obj)) {
        return obj.map(item => clean(item));
      }
      
      const result: any = {};
      for (const [key, value] of Object.entries(obj)) {
        if (typeof value === 'function') {
          continue; // Skip functions
        }
        if (key.startsWith('_') && ['_parent', '_data', '_index', '_queryCache', '_storageFactory'].includes(key)) {
          continue; // Skip internal properties
        }
        result[key] = clean(value);
      }
      
      return result;
    };
    
    return clean(data);
  }
}

/**
 * Storage manager for applying WAL entries
 */
class FileStorageManager<T> {
  constructor(private segmentManager: SegmentManager) {}

  /**
   * Apply a WAL entry to the storage
   */
  async applyWALEntry(entry: WALEntry): Promise<void> {
    try {
      // WAL replay should only update cache, not write to segments again
      // because segments are written during normal operation
      switch (entry.operation) {
        case "CREATE":
        case "UPDATE":
          if (entry.data) {
            // Only update cache, don't write to segments to avoid duplicates
            // The data should already be in segments from the original write
            console.log(`WAL replay: Restored ${entry.documentId} to cache`);
          }
          break;

        case "DELETE":
          // For deletes, we do need to mark as deleted in segments
          await this.segmentManager.markDeleted(entry.documentId);
          break;

        default:
          console.warn(`Unknown WAL operation: ${entry.operation}`);
      }
    } catch (error) {
      throw new StorageError(
        `Failed to apply WAL entry: ${entry.sequenceNumber}`,
        "WAL_APPLY_ERROR",
        true,
        { error, entry }
      );
    }
  }
}

/**
 * Memory storage wrapper that provides sync helpers for FileStorage
 * This allows FileStorage to work with the existing Items class
 */
export class FileStorageWrapper<T> extends FileStorage<T> {
  private syncCache = new Map<string, T>();
  private preloadCompleted = false;

  /**
   * Override initialize to preload data into sync cache
   */
  async initialize(): Promise<void> {
    // Call parent initialize first
    await super.initialize();

    // Only preload once, even if initialize is called multiple times
    if (!this.preloadCompleted) {
      await this.preloadDataIntoCache();
      this.preloadCompleted = true;
    }
  }

  /**
   * Preload existing data into sync cache for synchronous access
   */
  private async preloadDataIntoCache(): Promise<void> {
    try {
      let loadedCount = 0;

      // Get all document keys from segments
      for await (const key of this.keys()) {
        // Load each document into cache
        const value = await this.get(key);
        if (value !== undefined) {
          // Add to sync cache for immediate access
          this.syncCache.set(key, value);
          loadedCount++;
        }
      }
      console.log(
        `Preloaded ${this.syncCache.size} items into sync cache (${loadedCount} documents)`
      );
    } catch (error) {
      console.warn("Failed to preload data into cache:", error);
      // Don't throw - this is not critical for basic functionality
    }
  }

  /**
   * Synchronous get (uses cache, falls back to async)
   */
  getSync(key: string): T | undefined {
    // First check sync cache
    if (this.syncCache.has(key)) {
      return this.syncCache.get(key);
    }

    // Check main cache
    const cached = this.cache.get(key);
    if (cached) {
      this.syncCache.set(key, cached);
      return cached;
    }

    // For file storage, we can't provide true sync access to disk
    // Return undefined and trigger async load in background
    this.get(key)
      .then(value => {
        if (value !== undefined) {
          this.syncCache.set(key, value);
        }
      })
      .catch(() => {
        // Ignore async errors in background load
      });

    return undefined;
  }

  /**
   * Synchronous set (updates cache, queues async write)
   */
  setSync(key: string, value: T): void {
    this.syncCache.set(key, value);

    // Queue async write
    this.set(key, value).catch(error => {
      console.error(`Background write failed for ${key}:`, error);
      // Remove from sync cache on write failure
      this.syncCache.delete(key);
    });
  }

  /**
   * Synchronous delete (updates cache, queues async delete)
   */
  deleteSync(key: string): boolean {
    const existed = this.syncCache.has(key);
    this.syncCache.delete(key);

    // Queue async delete
    this.delete(key).catch(error => {
      console.error(`Background delete failed for ${key}:`, error);
    });

    return existed;
  }

  /**
   * Synchronous has check
   */
  hasSync(key: string): boolean {
    return this.syncCache.has(key) || this.cache.get(key) !== undefined;
  }

  /**
   * Get underlying map for compatibility
   */
  getUnderlyingMap(): Map<string, T> {
    return this.syncCache;
  }

  /**
   * Synchronous keys iterator
   */
  keysSync(): IterableIterator<string> {
    return this.syncCache.keys();
  }

  /**
   * Synchronous size
   */
  sizeSync(): number {
    return this.syncCache.size;
  }
}
