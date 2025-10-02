/* eslint-disable class-methods-use-this, no-underscore-dangle, no-use-before-define */
import { randomUUID } from "crypto";
import query, { CompositeIndex } from "@zeit/cosmosdb-query";
import LRU from "lru-cache";
import ItemObject from "./item-object";
import Item from "./item";
import getValue from "../utils/get-value";
import { PartitionValue } from "../types";
import { StorageFactory, StorageBackend } from "../storage/interfaces";
import { MemoryStorage } from "../storage/memory-storage";
import { FileStorageWrapper } from "../storage/file-storage";
import { UnifiedStorage } from "../storage/unified-storage";
import { PerformantStorage } from "../storage/performant-storage";
import { SegmentStorage } from "../storage/segment-storage";
import { RidManager } from "../storage/rid-manager";
import { debugLog } from "../debug";

type Query = ReturnType<typeof query>;

function ts() {
  return Math.floor(Date.now() / 1e3);
}

export default class Items<P extends Item, I extends Item> {
  _parent: P;

  _data: StorageBackend<I>;

  _index: StorageBackend<string>;

  _ridCount: number;

  _ridManager: RidManager;

  _queryCache: LRU<string, Query>;

  _partitionKeyPath: string[];

  _uniqueKeyPaths: string[];

  protected _storageFactory?: StorageFactory;

  constructor(
    parent: P,
    partitionKeyPath: string[],
    uniqueKeyPaths: string[] = [],
    storageFactory?: StorageFactory
  ) {
    this._parent = parent;
    this._storageFactory = storageFactory;
    
    // Initialize storage - simple approach that works
    this._data = this._createDataStorage();
    this._index = this._createIndexStorage();
    
    this._ridCount = 0;
    this._ridManager = RidManager.getInstance();
    this._queryCache = new LRU({ max: 100 });
    this._partitionKeyPath = partitionKeyPath;
    this._uniqueKeyPaths = uniqueKeyPaths.concat("/id");
    
    // Initialize RID manager from existing index storage to find max RID
    const namespace = this._getCollectionNamespace();
    this._ridManager.initializeFromStorage(this._index, namespace);
  }

  /**
   * Initialize ridCount from existing storage to prevent RID collisions after restart
   */
  private _initializeRidCount(): void {
    try {
      // For file storage, we need to scan existing data to find the highest RID count
      if (this._data instanceof FileStorageWrapper) {
        // Get all keys from storage synchronously
        const keys = Array.from(this._data.keysSync());
        let maxRidCount = 0;
        
        for (const key of keys) {
          try {
            const data = this._data.getSync(key);
            if (data && typeof data === 'object' && '_rid' in data) {
              // Extract the numeric part from the RID to find the highest count
              const ridCount = this._extractRidCount(data._rid as string);
              if (ridCount > maxRidCount) {
                maxRidCount = ridCount;
              }
            }
          } catch (error) {
            // Ignore errors for individual items
            console.warn(`Failed to extract RID from key ${key}:`, error);
          }
        }
        
        this._ridCount = maxRidCount;
        debugLog(`Initialized RID count to ${this._ridCount} from existing storage`);
      }
    } catch (error) {
      console.warn('Failed to initialize RID count from storage:', error);
      this._ridCount = 0;
    }
  }

  /**
   * Extract numeric RID count from RID string for initialization
   */
  private _extractRidCount(rid: string): number {
    try {
      // This is a simple extraction - could be improved based on RID format
      // For now, just use a timestamp-based approach as fallback
      return Math.floor(Date.now() / 1000) % 1000000;
    } catch (error) {
      return 0;
    }
  }

  /**
   * Create data storage
   */
  private _createDataStorage(): StorageBackend<I> {
    if (this._storageFactory) {
      const collectionRid = this._getCollectionRid();
      return this._storageFactory.createItemStorage<I>(collectionRid);
    } else {
      return new MemoryStorage<I>();
    }
  }

  /**
   * Create index storage
   */
  private _createIndexStorage(): StorageBackend<string> {
    if (this._storageFactory) {
      const collectionRid = this._getCollectionRid();
      return this._storageFactory.createIndexStorage(collectionRid);
    } else {
      return new MemoryStorage<string>();
    }
  }

  /**
   * Generate collection namespace for proper isolation
   */
  private _getCollectionNamespace(): string {
    if (this._parent && typeof this._parent.read === "function") {
      const parentData = this._parent.read();
      if (parentData && parentData._rid) {
        return `collection-${parentData._rid}`;
      }
    }
    return `collection-default`;
  }

  /**
   * Get collection RID for storage isolation
   */
  private _getCollectionRid(): string {
    // For databases (parent is Account), use a special namespace
    if (this._parent && this._parent.constructor.name === 'Account') {
      return '_databases';
    }
    
    // For collections (parent is Database), use database-specific namespace
    if (this._parent && this._parent.constructor.name === 'Database') {
      const parentData = this._parent.read();
      if (parentData && parentData._rid) {
        return `_collections_${parentData._rid}`;
      }
      return '_collections_default';
    }
    
    // For documents (parent is Collection), use collection-specific namespace
    if (this._parent && typeof this._parent.read === "function") {
      const parentData = this._parent.read();
      if (parentData && parentData._rid) {
        return parentData._rid;
      }
    }
    
    return "default";
  }

  /**
   * Helper to get sync access to storage
   */
  private _getSync<T>(storage: StorageBackend<T>, key: string): T | undefined {
    if (storage instanceof MemoryStorage || storage instanceof FileStorageWrapper || storage instanceof UnifiedStorage || storage instanceof PerformantStorage || storage instanceof SegmentStorage) {
      return storage.getSync(key);
    }
    throw new Error("Sync access only supported for MemoryStorage, FileStorageWrapper, UnifiedStorage, PerformantStorage, and SegmentStorage");
  }

  private _setSync(storage: StorageBackend<any>, key: string, value: any): void {
    try {
      if (storage instanceof MemoryStorage || storage instanceof FileStorageWrapper || storage instanceof UnifiedStorage || storage instanceof PerformantStorage || storage instanceof SegmentStorage) {
        storage.setSync(key, value);
      } else {
        storage.set(key, value).catch(console.error);
      }
    } catch (error) {
      console.error(`Storage failed for key=${key}:`, error);
    }
  }

  private _deleteSync(storage: StorageBackend<any>, key: string): void {
    if (storage instanceof MemoryStorage || storage instanceof FileStorageWrapper || storage instanceof UnifiedStorage || storage instanceof PerformantStorage || storage instanceof SegmentStorage) {
      storage.deleteSync(key);
    } else {
      // Fallback to async (fire and forget)
      storage.delete(key).catch(console.error);
    }
  }

  private _entriesSync<T>(storage: StorageBackend<T>): IterableIterator<[string, T]> {
    if (storage instanceof MemoryStorage) {
      return storage.getUnderlyingMap().entries();
    } else if (storage instanceof FileStorageWrapper) {
      return storage.getUnderlyingMap().entries();
    } else if (storage instanceof UnifiedStorage) {
      return storage.entriesSync()[Symbol.iterator]();
    } else if (storage instanceof PerformantStorage) {
      return storage.entriesSync()[Symbol.iterator]();
    } else if (storage instanceof SegmentStorage) {
      return storage.entriesSync()[Symbol.iterator]();
    } else {
      throw new Error("Sync access only supported for MemoryStorage, FileStorageWrapper, UnifiedStorage, PerformantStorage, and SegmentStorage");
    }
  }

  create(data: { [x: string]: any }) {
    const partition = this._getPartition(data);
    this._uniqueKeyPaths.forEach(keyPath => {
      const keyPathValue = getValue(keyPath.slice(1).split("/"), data);
      if (this._item(keyPath, keyPathValue, partition).read()) {
        const err = new Error(
          `Resource with specified ${keyPath.slice(1)} already exists.`
        );
        // @ts-ignore
        err.conflict = true;
        throw err;
      }
    });

    const namespace = this._getCollectionNamespace();
    const _rid = this._ridManager.generateDocumentRid(namespace);
    const _data = {
      ...data,
      id: data.id,
      _etag: randomUUID(),
      _rid,
      _self: this._self(_rid),
      _ts: ts()
    };
    return this._set(_data);
  }

  delete(idOrRid: string, partition: PartitionValue) {
    const data = this._item("/id", idOrRid, partition).read();
    if (!data) {
      throw new Error("does not exist");
    }

    this._uniqueKeyPaths.forEach(keyPath => {
      const keyPathValue = getValue(keyPath.slice(1).split("/"), data);
      const id = this._getId(keyPath, keyPathValue, this._getPartition(data));
      this._deleteSync(this._index, id);
    });
    this._deleteSync(this._data, data._rid);
  }

  query(
    params: {
      query: string;
      parameters?: {
        name: string;
        value: any;
      }[];
    },
    {
      maxItemCount,
      continuation
    }: {
      maxItemCount?: number | null;
      continuation?: {
        token: string;
      } | null;
    }
  ) {
    if (!this._parent.read()) return null;

    // Filter data by collection namespace for proper isolation
    // Use optimized storage query if available
    if (this._data instanceof PerformantStorage) {
      // Get all documents from performant storage efficiently
      const documents = (this._data as any).getAllDocumentsSync();
      debugLog(`PerformantStorage query found ${documents.length} documents`);
      
      const udf = this._userDefinedFunctions();
      const parent = this._parent.read();
      const compositeIndexes = ((parent.indexingPolicy
        ? parent.indexingPolicy.compositeIndexes
        : null) || []) as CompositeIndex[][];
      const q = this._getQuery(params.query);
      const options = {
        parameters: params.parameters,
        udf,
        maxItemCount,
        continuation,
        compositeIndexes
      };
      try {
        return q.exec(documents, options);
      } catch (err) {
        // @ts-ignore
        err.badRequest = true;
        throw err;
      }
    }

    // Use segment storage for high-performance queries
    if (this._data instanceof SegmentStorage) {
      const documents = this._data.valuesSync();
      debugLog(`SegmentStorage query found ${documents.length} documents`);
      
      const udf = this._userDefinedFunctions();
      const parent = this._parent.read();
      const compositeIndexes = ((parent.indexingPolicy
        ? parent.indexingPolicy.compositeIndexes
        : null) || []) as CompositeIndex[][];
      const q = this._getQuery(params.query);
      const options = {
        parameters: params.parameters,
        udf,
        maxItemCount,
        continuation,
        compositeIndexes
      };
      try {
        return q.exec(documents, options);
      } catch (err) {
        // @ts-ignore
        err.badRequest = true;
        throw err;
      }
    }

    // Fallback for other storage types
    const collectionNamespace = this._getCollectionNamespace();
    const filteredRids: string[] = [];
    
        // Get RIDs that belong to this collection by checking the index
    for (const [indexKey, rid] of this._entriesSync(this._index)) {
      if (indexKey.startsWith(`${collectionNamespace}:`)) {
        filteredRids.push(rid);
      }
    }
    
    // Deduplicate RIDs to prevent duplicate documents in results
    const uniqueRids = [...new Set(filteredRids)];
    
    // Get the actual data objects using the filtered RIDs
    const data = uniqueRids
      .map(rid => this._getSync(this._data, rid))
      .filter(item => item != null)
      .map(item => {
        // If item has a read method, use it; otherwise, it's raw data
        if (item && typeof item === 'object' && 'read' in item && typeof item.read === 'function') {
          return item.read();
        }
        // It's raw document data, return as-is
        return item;
      });
    
    const udf = this._userDefinedFunctions();
    const parent = this._parent.read();
    const compositeIndexes = ((parent.indexingPolicy
      ? parent.indexingPolicy.compositeIndexes
      : null) || []) as CompositeIndex[][];
    const q = this._getQuery(params.query);
    const options = {
      parameters: params.parameters,
      udf,
      maxItemCount,
      continuation,
      compositeIndexes
    };
    try {
      return q.exec(data, options);
    } catch (err) {
      // @ts-ignore
      err.badRequest = true;
      throw err;
    }
  }

  read({
    maxItemCount,
    continuation
  }: {
    maxItemCount?: number | null;
    continuation?: {
      token: string;
    } | null;
  } = {}) {
    if (!this._parent.read()) return null;

    // Filter data by collection namespace for proper isolation
    const collectionNamespace = this._getCollectionNamespace();
    const filteredRids: string[] = [];
    
    // Get RIDs that belong to this collection by checking the index
    for (const [indexKey, rid] of this._entriesSync(this._index)) {
      if (indexKey.startsWith(`${collectionNamespace}:`)) {
        filteredRids.push(rid);
      }
    }
    
    // Get the actual data objects using the filtered RIDs
    const data = filteredRids
      .map(rid => this._getSync(this._data, rid))
      .filter(item => item != null)
      .map(item => {
        // If item has a read method, use it; otherwise, it's raw data
        if (item && typeof item === 'object' && 'read' in item && typeof item.read === 'function') {
          return item.read();
        }
        // It's raw document data, return as-is
        return item;
      });
    return this._getQuery("SELECT * FROM c").exec(data, {
      maxItemCount,
      continuation
    });
  }

  replace(data: { [x: string]: any }, original: { [x: string]: any }) {
    if (this._getPartition(data) !== this._getPartition(original)) {
      const err = new Error(
        `replacing partition key "${this._getPartitionKeyPath().join(
          "."
        )}" is not allowed`
      );
      // @ts-ignore
      err.badRequest = true;
      throw err;
    }

    const partition = this._getPartition(data);
    const oldData = this._item("/id", data.id, partition).read();
    if (!oldData || data.id !== oldData.id) {
      const err = new Error("replacing id is not allowed");
      // @ts-ignore
      err.badRequest = true;
      throw err;
    }

    this._uniqueKeyPaths.forEach(keyPath => {
      // No need to check /id here since we explicitly checked it earlier
      if (keyPath === "/id") {
        return;
      }

      const keys = keyPath.slice(1).split("/");
      const newKeyPathValue = getValue(keys, data);
      const originalKeyPathValue = getValue(keys, original);
      if (newKeyPathValue === originalKeyPathValue) {
        return;
      }

      if (this._item(keyPath, newKeyPathValue, partition).read()) {
        const err = new Error(
          `Resource with specified ${keyPath.slice(1)} already exists.`
        );
        // @ts-ignore
        err.conflict = true;
        throw err;
      }
    });

    const { _rid, _self } = oldData;
    const _data = {
      ...data,
      id: data.id,
      _etag: randomUUID(),
      _rid,
      _self,
      _ts: ts()
    };
    this.delete(oldData.id, partition);
    return this._set(_data);
  }

  upsert(data: { [x: string]: any }) {
    const partition = this._getPartition(data);
    const oldData = this._item("/id", data.id, partition).read();
    return oldData ? this.replace(data, oldData) : this.create(data);
  }

  _item(keyPath: string, keyPathValue: string, partition: PartitionValue): I {
    const rid = this._toRid(keyPath, keyPathValue, partition);
    const data = this._getSync(this._data, rid);
    
    // If we found data, it should be a proper object instance
    if (data) {
      // Check if it's already a proper instance with the expected methods
      if (typeof data === 'object' && data.constructor && data.constructor !== Object) {
        return data as I;
      }
      
      // The data from storage should be raw document data (ItemObject)
      // We need to pass it to _newItem to create the proper instance
      return this._newItem(data as unknown as ItemObject);
    }
    
    return this._newItem();
  }

  // eslint-disable-next-line no-unused-vars
  _newItem(data?: ItemObject): I {
    throw new Error("Not implemented");
  }

  _rid(id: number): string {
    throw new Error("Not implemented");
  }

  // eslint-disable-next-line no-unused-vars
  _self(rid: string): string {
    throw new Error("Not implemented");
  }

  _set(data: ItemObject) {
    this._uniqueKeyPaths.forEach(keyPath => {
      const keyPathValue = getValue(keyPath.slice(1).split("/"), data);
      const id = this._getId(keyPath, keyPathValue, this._getPartition(data));
      this._setSync(this._index, id, data._rid);
    });
    
    this._setSync(this._data, data._rid, data as any);
    return data;
  }

  _toRid(
    keyPath: string,
    keyPathValueOrRid: string,
    partition: PartitionValue
  ) {
    const id = this._getId(keyPath, keyPathValueOrRid, partition);
    return this._getSync(this._index, id) || keyPathValueOrRid;
  }

  _userDefinedFunctions():
    | {
        [x: string]: any;
      }
    | undefined
    | null {
    return null;
  }

  _getQuery(sql: string): Query {
    let q = this._queryCache.get(sql);
    if (!q) {
      q = query(sql);
      this._queryCache.set(sql, q);
    }
    return q;
  }

  _getId(keyPath: string, id: string, partition: PartitionValue) {
    const collectionNamespace = this._getCollectionNamespace();
    return `${collectionNamespace}:${[partition || id, keyPath, id].join(":")}`;
  }

  _getPartition(data: { [x: string]: any }): PartitionValue {
    return getValue(this._getPartitionKeyPath(), data);
  }

  _getPartitionKeyPath(): string[] {
    const [firstPath] = this._partitionKeyPath;
    if (!firstPath) {
      return ["id"]; // Default/fallback to `id` as partition key for now
    }
    const path = firstPath.slice(1).split("/");
    if (path.length === 1 && path[0] === "_partitionKey") {
      return ["id"];
    }
    return path;
  }
}
