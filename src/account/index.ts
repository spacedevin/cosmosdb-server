/* eslint-disable class-methods-use-this, no-underscore-dangle */
import Databases from "./databases";
import Item from "./item";
import { StorageFactory } from "../storage/interfaces";
import { debugLog } from "../debug";

export default class Account extends Item {
  databases: Databases;
  private storageFactory?: StorageFactory;

  constructor(host: string, port: number, storageFactory?: StorageFactory) {
    super({
      _rid: host,
      _self: "",
      _dbs: "//dbs/",
      id: host,
      media: "//media/",
      addresses: "//addresses/",
      writableLocations: [
        {
          name: "South Central US",
          databaseAccountEndpoint: `https://${host}:${port}/`
        }
      ],
      readableLocations: [
        {
          name: "South Central US",
          databaseAccountEndpoint: `https://${host}:${port}/`
        }
      ],
      enableMultipleWriteLocations: false,
      userReplicationPolicy: {
        asyncReplication: false,
        minReplicaSetSize: 1,
        maxReplicasetSize: 4
      },
      userConsistencyPolicy: {
        defaultConsistencyLevel: "Session"
      },
      systemReplicationPolicy: {
        minReplicaSetSize: 1,
        maxReplicasetSize: 4
      },
      readPolicy: {
        primaryReadCoefficient: 1,
        secondaryReadCoefficient: 1
      },
      queryEngineConfiguration:
        '{"maxSqlQueryInputLength":262144,"maxJoinsPerSqlQuery":5,"maxLogicalAndPerSqlQuery":500,"maxLogicalOrPerSqlQuery":500,"maxUdfRefPerSqlQuery":10,"maxInExpressionItemsCount":16000,"queryMaxInMemorySortDocumentCount":500,"maxQueryRequestTimeoutFraction":0.9,"sqlAllowNonFiniteNumbers":false,"sqlAllowAggregateFunctions":true,"sqlAllowSubQuery":true,"sqlAllowScalarSubQuery":true,"allowNewKeywords":true,"sqlAllowLike":false,"maxSpatialQueryCells":12,"spatialMaxGeometryPointCount":256,"sqlAllowTop":true,"enableSpatialIndexing":true}'
    });
    this.storageFactory = storageFactory;
    this.databases = new Databases(this, ["/id"], [], storageFactory);
  }

  /**
   * Initialize account and restore data from persistent storage
   */
  async initialize(): Promise<void> {
    if (!this.storageFactory) {
      // Memory storage - no restoration needed
      return;
    }

    try {
      // Initialize storage factory (this will handle WAL replay)
      if (typeof this.storageFactory.initialize === 'function') {
        await this.storageFactory.initialize();
      }

      // Restore databases and collections from file storage
      await this.restoreFromStorage();

      debugLog('Account initialized with file storage');
    } catch (error) {
      console.warn('Failed to initialize persistent storage, falling back to memory mode:', error);
    }
  }

  /**
   * Restore databases and collections from storage
   */
  private async restoreFromStorage(): Promise<void> {
    if (!this.storageFactory) return;

    try {
      // Initialize database storage to ensure databases are loaded
      const databaseStorage = this.storageFactory.createItemStorage('_databases');
      if (typeof databaseStorage.initialize === 'function') {
        await databaseStorage.initialize();
      }

      // Check if there are any collection directories
      const fs = require('fs').promises;
      const path = require('path');
      const dataDir = './cosmos-local-data/collections';

      try {
        const collections = await fs.readdir(dataDir);
        debugLog(`Found ${collections.length} storage namespaces to check`);

        // Look for databases storage
        if (collections.includes('X2RhdGFiYXNlcw__')) { // Base64 encoded '_databases'
          debugLog('Found databases storage namespace');
        }

        // Look for collections storage (starts with '_collections_')
        const collectionStorages = collections.filter((name: string) => {
          try {
            const decoded = this.decodeCollectionRid(name);
            return decoded.startsWith('_collections_');
          } catch {
            return false;
          }
        });
        
        if (collectionStorages.length > 0) {
          debugLog(`Found ${collectionStorages.length} collection storage namespaces`);
        }

        // Look for document collections (actual collection RIDs)
        const documentCollections = collections.filter((name: string) => {
          try {
            const decoded = this.decodeCollectionRid(name);
            return !decoded.startsWith('_');
          } catch {
            return false;
          }
        });
        
        if (documentCollections.length > 0) {
          debugLog(`Found ${documentCollections.length} document collection storage namespaces`);
        }

      } catch (error) {
        // Collections directory doesn't exist yet - that's fine
        debugLog('No existing storage found');
      }
    } catch (error) {
      console.warn('Failed to restore from storage:', error);
    }
  }

  /**
   * Decode collection RID from sanitized directory name
   */
  private decodeCollectionRid(dirName: string): string {
    try {
      // Reverse the sanitization process
      const base64 = dirName.replace(/_/g, '=').replace(/-/g, '/').replace(/\./g, '+');
      return Buffer.from(base64, 'base64').toString('utf8');
    } catch (error) {
      console.warn(`Failed to decode collection RID from ${dirName}:`, error);
      return dirName; // Fallback to original name
    }
  }

  database(idOrRid: string) {
    return this.databases._item("/id", idOrRid, idOrRid);
  }
}
