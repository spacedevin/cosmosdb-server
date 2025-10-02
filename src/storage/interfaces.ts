/**
 * Storage backend interfaces and types for cosmos-local persistent storage
 */

/**
 * Generic storage backend interface that provides Map-like operations
 * with async support for persistent storage implementations
 */
export interface StorageBackend<T> {
  get(key: string): Promise<T | undefined>;
  set(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  has(key: string): Promise<boolean>;
  keys(): AsyncIterableIterator<string>;
  clear(): Promise<void>;
  size(): Promise<number>;
  
  // Optional lifecycle methods
  initialize?(): Promise<void>;
  close?(): Promise<void>;
}

/**
 * Configuration options for storage backends
 */
export interface StorageOptions {
  dataDir?: string;
  compression?: boolean;
  cacheSize?: number;
  syncMode?: "immediate" | "batched" | "lazy";
  collectionRid?: string; // Collection RID for directory isolation
  
  // WAL specific options
  walEnabled?: boolean;
  maxWalSegmentSize?: number;
  
  // Performance options
  readCacheSize?: number;
  writeBatchSize?: number;
  compactionInterval?: number;
}

/**
 * Storage performance metrics
 */
export interface StorageMetrics {
  operationsPerSecond: number;
  averageLatency: number;
  errorRate: number;
  memoryUsage: number;
  diskUsage?: number;
  cacheHitRatio?: number;
}

/**
 * Storage operation result with success/failure information
 */
export interface StorageResult<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  retryable?: boolean;
}

/**
 * Storage factory interface for creating different storage backends
 */
export interface StorageFactory {
  createItemStorage<T>(collectionRid?: string): StorageBackend<T>;
  createIndexStorage(collectionRid?: string): StorageBackend<string>;
  getMetrics(): StorageMetrics;
  initialize?(): Promise<void>;
  close(): Promise<void>;
}

/**
 * Storage mode enumeration
 */
export type StorageMode = 'memory' | 'file' | 'hybrid';

/**
 * Storage configuration loaded from environment or CLI
 */
export interface StorageConfig {
  mode: StorageMode;
  dataDir: string;
  persistence: boolean;
  compression: boolean;
  cacheSize: number;
  walEnabled: boolean;
}
