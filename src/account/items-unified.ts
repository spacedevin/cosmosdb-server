/* eslint-disable class-methods-use-this, no-underscore-dangle, no-use-before-define */
import { randomUUID } from "crypto";
import query, { CompositeIndex } from "@zeit/cosmosdb-query";
import LRU from "lru-cache";
import ItemObject from "./item-object";
import Item from "./item";
import getValue from "../utils/get-value";
import { PartitionValue } from "../types";
import { StorageFactory, StorageBackend } from "../storage/interfaces";

type Query = ReturnType<typeof query>;

function ts() {
  return Math.floor(Date.now() / 1e3);
}

export default class Items<P extends Item, I extends Item> {
  _parent: P;
  _storage: StorageBackend<any>; // Single unified storage
  _ridCount: number;
  _queryCache: LRU<string, Query>;
  _partitionKeyPath: string[];
  _collectionNamespace: string;

  constructor(parent: P, storageFactory: StorageFactory, partitionKeyPath: string[] = []) {
    this._parent = parent;
    this._storage = storageFactory.createItemStorage(); // Single unified storage
    this._ridCount = 0;
    this._queryCache = new LRU<string, Query>({ max: 100 });
    this._partitionKeyPath = partitionKeyPath;
    
    // Create unique namespace for this collection
    this._collectionNamespace = this._getCollectionNamespace();
    
    // Initialize RID count from existing data
    this._initializeRidCount();
  }

  private _getCollectionNamespace(): string {
    const parent = this._parent.read();
    if (!parent) return 'unknown';
    
    // Use database and collection info to create unique namespace
    const databaseId = parent._parent?.read()?._self || 'db';
    const collectionId = parent._self || 'collection';
    return `${databaseId}:${collectionId}`;
  }

  private _initializeRidCount(): void {
    // Count existing documents to set initial RID count
    let maxRid = 0;
    
    try {
      const keys = this._storage.keysSync();
      for (const key of keys) {
        if (key.startsWith(`${this._collectionNamespace}:data:`)) {
          const item = this._storage.getSync(key);
          if (item && item._rid) {
            // Extract RID number from base64 encoded RID
            const ridNumber = this._extractRidNumber(item._rid);
            if (ridNumber > maxRid) {
              maxRid = ridNumber;
            }
          }
        }
      }
      
      this._ridCount = maxRid + 1;
      console.log(`Initialized RID count to ${this._ridCount} for ${this._collectionNamespace}`);
    } catch (error) {
      console.warn(`Failed to initialize RID count for ${this._collectionNamespace}:`, error);
      this._ridCount = 1;
    }
  }

  private _extractRidNumber(rid: string): number {
    try {
      // Decode base64 RID and extract numeric part
      const decoded = Buffer.from(rid, 'base64');
      // Read as big-endian 64-bit integer, take lower 32 bits
      const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
      if (view.byteLength >= 8) {
        return view.getUint32(4, false); // Big-endian, second 32 bits
      }
      return 0;
    } catch {
      return 0;
    }
  }

  private _createKey(type: 'data' | 'index', id: string): string {
    return `${this._collectionNamespace}:${type}:${id}`;
  }

  private _generateRid(): string {
    const ridNumber = this._ridCount++;
    
    // Create a proper CosmosDB-style RID
    const buffer = Buffer.alloc(8);
    buffer.writeUInt32BE(0x80000004, 0); // Fixed prefix for document RIDs
    buffer.writeUInt32BE(ridNumber, 4);   // Incremental number
    
    return buffer.toString('base64');
  }

  add(data: I): void {
    if (!this._parent.read()) return;

    // Generate RID if not present
    if (!data._rid) {
      data._rid = this._generateRid();
    }

    // Set timestamps
    data._ts = ts();

    // Store document data
    const dataKey = this._createKey('data', data._rid);
    this._storage.setSync(dataKey, data);

    // Create indexes for query optimization
    this._createIndexes(data);
  }

  private _createIndexes(data: I): void {
    const dataObj = data.read();
    
    // Create primary index by document ID
    if (dataObj.id) {
      const indexKey = this._createKey('index', `id:${dataObj.id}`);
      this._storage.setSync(indexKey, data._rid);
    }

    // Create indexes for partition key path
    if (this._partitionKeyPath.length > 0) {
      const partitionValue = getValue(this._partitionKeyPath, dataObj);
      if (partitionValue !== undefined) {
        const indexKey = this._createKey('index', `partition:${JSON.stringify(partitionValue)}`);
        this._storage.setSync(indexKey, data._rid);
      }
    }

    // Create full-text search indexes for common fields
    for (const [key, value] of Object.entries(dataObj)) {
      if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        const indexKey = this._createKey('index', `field:${key}:${value}`);
        this._storage.setSync(indexKey, data._rid);
      }
    }
  }

  update(data: I): void {
    if (!this._parent.read() || !data._rid) return;

    // Update timestamp
    data._ts = ts();

    // Update document data
    const dataKey = this._createKey('data', data._rid);
    this._storage.setSync(dataKey, data);

    // Update indexes
    this._createIndexes(data);
  }

  private _removeIndexes(data: I): void {
    const dataObj = data.read();
    
    // Remove primary index
    if (dataObj.id) {
      const indexKey = this._createKey('index', `id:${dataObj.id}`);
      this._storage.deleteSync(indexKey);
    }

    // Remove partition key index
    if (this._partitionKeyPath.length > 0) {
      const partitionValue = getValue(this._partitionKeyPath, dataObj);
      if (partitionValue !== undefined) {
        const indexKey = this._createKey('index', `partition:${JSON.stringify(partitionValue)}`);
        this._storage.deleteSync(indexKey);
      }
    }

    // Remove field indexes
    for (const [key, value] of Object.entries(dataObj)) {
      if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        const indexKey = this._createKey('index', `field:${key}:${value}`);
        this._storage.deleteSync(indexKey);
      }
    }
  }

  remove(data: I): void {
    if (!this._parent.read() || !data._rid) return;

    // Remove indexes first
    this._removeIndexes(data);

    // Remove document data
    const dataKey = this._createKey('data', data._rid);
    this._storage.deleteSync(dataKey);
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

    // Get all documents for this collection
    const documents: any[] = [];
    const keys = this._storage.keysSync();
    
    for (const key of keys) {
      if (key.startsWith(`${this._collectionNamespace}:data:`)) {
        const item = this._storage.getSync(key);
        if (item && typeof item === 'object' && 'read' in item && typeof item.read === 'function') {
          documents.push(item.read());
        } else if (item) {
          documents.push(item);
        }
      }
    }

    console.log(`Query found ${documents.length} documents for ${this._collectionNamespace}`);

    // Get query engine
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
      console.error('Query execution error:', err);
      return null;
    }
  }

  // ... rest of the methods remain the same for now ...
  private _userDefinedFunctions() {
    return {};
  }

  private _getQuery(queryString: string): Query {
    if (this._queryCache.has(queryString)) {
      return this._queryCache.get(queryString)!;
    }
    
    const q = query(queryString);
    this._queryCache.set(queryString, q);
    return q;
  }

  private _getPartition(data: any): string {
    if (this._partitionKeyPath.length === 0) return "";
    const value = getValue(this._partitionKeyPath, data);
    return value !== undefined ? String(value) : "";
  }

  private _getId(keyPath: string, keyPathValue: any, partition: string): string {
    return `${this._collectionNamespace}:${keyPath}:${keyPathValue}:${partition}`;
  }
}