/* eslint-disable class-methods-use-this, no-underscore-dangle, no-use-before-define */
import { randomUUID } from "crypto";
import query, { CompositeIndex } from "@zeit/cosmosdb-query";
import LRU from "lru-cache";
import ItemObject from "./item-object";
import Item from "./item";
import getValue from "../utils/get-value";
import { PartitionValue } from "../types";
import { StorageFactory } from "../storage/interfaces";
import { UnifiedStorage } from "../storage/unified-storage";

type Query = ReturnType<typeof query>;

function ts() {
  return Math.floor(Date.now() / 1e3);
}

export default class ItemsSimplified<P extends Item, I extends Item> {
  _parent: P;
  _storage: UnifiedStorage<any>;
  _ridCount: number;
  _queryCache: LRU<string, Query>;
  _partitionKeyPath: string[];
  _collectionNamespace: string;

  constructor(parent: P, storageFactory: StorageFactory, partitionKeyPath: string[] = []) {
    this._parent = parent;
    this._storage = storageFactory.createItemStorage() as UnifiedStorage<any>;
    this._queryCache = new LRU<string, Query>({ max: 100 });
    this._partitionKeyPath = partitionKeyPath;
    
    // Create unique namespace for this collection
    this._collectionNamespace = this._getCollectionNamespace();
    
    // Initialize RID count from existing data
    this._ridCount = this._initializeRidCount();
    
    console.log(`Items initialized for ${this._collectionNamespace} with RID count starting at ${this._ridCount}`);
  }

  private _getCollectionNamespace(): string {
    const parent = this._parent.read();
    if (!parent) return 'unknown';
    
    const databaseRid = parent._parent?.read()?._rid || 'db';
    const collectionRid = parent._rid || 'collection';
    return `${databaseRid}:${collectionRid}`;
  }

  private _initializeRidCount(): number {
    let maxRid = 0;
    
    try {
      const keys = this._storage.keysSync();
      const docPrefix = `${this._collectionNamespace}:doc:`;
      
      for (const key of keys) {
        if (key.startsWith(docPrefix)) {
          const doc = this._storage.getSync(key);
          if (doc && doc._rid) {
            const ridNumber = this._extractRidNumber(doc._rid);
            if (ridNumber > maxRid) {
              maxRid = ridNumber;
            }
          }
        }
      }
      
      console.log(`Found ${maxRid} existing documents in ${this._collectionNamespace}`);
      return maxRid + 1;
    } catch (error) {
      console.warn(`Failed to initialize RID count:`, error);
      return 1;
    }
  }

  private _extractRidNumber(rid: string): number {
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

    const doc = data as any;
    const docData = data.read();

    // Generate unique RID
    if (!doc._rid) {
      doc._rid = this._generateRid();
    }

    // Set timestamp
    doc._ts = ts();

    // Store document with unique key based on document ID
    const documentKey = `${this._collectionNamespace}:doc:${docData.id}`;
    
    // Store the complete document data including metadata
    const fullDoc = {
      ...docData,
      _rid: doc._rid,
      _ts: doc._ts,
      _etag: docData._etag || randomUUID(),
      _self: `/dbs/${this._parent.read()?._parent?.read()?._rid}/colls/${this._parent.read()?._rid}/docs/${doc._rid}/`
    };

    this._storage.setSync(documentKey, fullDoc);
    
    console.log(`Stored document ${docData.id} with RID ${doc._rid} in ${this._collectionNamespace}`);
  }

  update(data: I): void {
    if (!this._parent.read()) return;

    const doc = data as any;
    const docData = data.read();

    // Update timestamp
    doc._ts = ts();

    // Update the document
    const documentKey = `${this._collectionNamespace}:doc:${docData.id}`;
    
    const fullDoc = {
      ...docData,
      _rid: doc._rid,
      _ts: doc._ts,
      _etag: randomUUID(),
      _self: `/dbs/${this._parent.read()?._parent?.read()?._rid}/colls/${this._parent.read()?._rid}/docs/${doc._rid}/`
    };

    this._storage.setSync(documentKey, fullDoc);
    
    console.log(`Updated document ${docData.id} with RID ${doc._rid}`);
  }

  remove(data: I): void {
    if (!this._parent.read()) return;

    const docData = data.read();
    const documentKey = `${this._collectionNamespace}:doc:${docData.id}`;
    
    this._storage.deleteSync(documentKey);
    
    console.log(`Removed document ${docData.id}`);
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
    const docPrefix = `${this._collectionNamespace}:doc:`;
    
    for (const key of keys) {
      if (key.startsWith(docPrefix)) {
        const doc = this._storage.getSync(key);
        if (doc) {
          documents.push(doc);
        }
      }
    }

    console.log(`Query found ${documents.length} documents for ${this._collectionNamespace}`);

    if (documents.length === 0) {
      return { resources: [], count: 0 };
    }

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
      const result = q.exec(documents, options);
      console.log(`Query executed successfully, returning ${result.resources?.length || 0} results`);
      return result;
    } catch (err) {
      console.error('Query execution error:', err);
      return { resources: [], count: 0 };
    }
  }

  queryItems(query: string, partition?: PartitionValue): ItemObject[] {
    if (!this._parent.read()) return [];

    const documents: any[] = [];
    const keys = this._storage.keysSync();
    const docPrefix = `${this._collectionNamespace}:doc:`;
    
    for (const key of keys) {
      if (key.startsWith(docPrefix)) {
        const doc = this._storage.getSync(key);
        if (doc) {
          documents.push(new ItemObject(doc, doc._rid, doc._ts));
        }
      }
    }

    return documents;
  }

  readItems(): ItemObject[] {
    return this.queryItems("");
  }

  // Compatibility methods
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