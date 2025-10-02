import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { StorageBackend } from './interfaces';

/**
 * High-performance storage engine with proper segmentation
 * - Database-level separation
 * - Collection-level separation  
 * - Partition-key based sharding
 * - Individual document files
 * - Proper indexing
 */
export class PerformantStorage<T> implements StorageBackend<T> {
  private dataDir: string;
  private databaseId: string;
  private collectionId: string;
  private cache = new Map<string, T>();
  private indexCache = new Map<string, Map<string, string>>();
  private isInitialized = false;

  constructor(config: {
    dataDir: string;
    databaseId: string;
    collectionId: string;
    cacheSize?: number;
  }) {
    this.dataDir = config.dataDir;
    this.databaseId = config.databaseId;
    this.collectionId = config.collectionId;
  }

  async initialize(): Promise<void> {
    if (this.isInitialized) return;

    // Create directory structure
    await this.ensureDirectoryStructure();
    
    // Load existing indexes
    await this.loadIndexes();
    
    // Load existing documents into cache and rebuild indexes
    await this.loadExistingDocuments();
    
    // Replay WAL for crash recovery
    await this.replayWal();
    
    this.isInitialized = true;
    console.log(`PerformantStorage initialized for ${this.databaseId}/${this.collectionId} with ${this.cache.size} documents`);
  }

  private async ensureDirectoryStructure(): Promise<void> {
    const paths = [
      this.getDatabasePath(),
      this.getCollectionPath(),
      this.getPartitionsPath(),
      this.getGlobalIndexesPath(),
      this.getWalPath()
    ];

    for (const dirPath of paths) {
      if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
      }
    }
  }

  private getDatabasePath(): string {
    return path.join(this.dataDir, 'databases', this.databaseId);
  }

  private getCollectionPath(): string {
    return path.join(this.getDatabasePath(), 'collections', this.collectionId);
  }

  private getPartitionsPath(): string {
    return path.join(this.getCollectionPath(), 'partitions');
  }

  private getGlobalIndexesPath(): string {
    return path.join(this.getCollectionPath(), 'global-indexes');
  }

  private getWalPath(): string {
    return path.join(this.getCollectionPath(), 'wal');
  }

  private getPartitionPath(partitionKey: string): string {
    const partitionHash = this.hashPartitionKey(partitionKey);
    return path.join(this.getPartitionsPath(), partitionHash);
  }

  private getDocumentPath(partitionKey: string, documentId: string): string {
    const partitionPath = this.getPartitionPath(partitionKey);
    const sanitizedDocId = this.sanitizeFilename(documentId);
    return path.join(partitionPath, 'documents', `${sanitizedDocId}.json`);
  }

  private sanitizeFilename(filename: string): string {
    // Replace invalid filename characters with safe alternatives
    return filename
      .replace(/[<>:"/\\|?*]/g, '_') // Replace invalid chars with underscore
      .replace(/\s+/g, '_')          // Replace spaces with underscore
      .substring(0, 200);            // Limit length to prevent filesystem issues
  }

  private hashPartitionKey(partitionKey: string): string {
    return crypto.createHash('sha256').update(partitionKey).digest('hex').slice(0, 8);
  }

  private async loadIndexes(): Promise<void> {
    try {
      // Load primary index (_id -> partition/document)
      const primaryIndexPath = path.join(this.getGlobalIndexesPath(), '_id.idx');
      if (fs.existsSync(primaryIndexPath)) {
        const indexData = JSON.parse(fs.readFileSync(primaryIndexPath, 'utf8'));
        this.indexCache.set('_id', new Map(Object.entries(indexData)));
        console.log(`Loaded primary index with ${Object.keys(indexData).length} entries`);
      }
    } catch (error) {
      console.warn('Failed to load indexes:', error);
    }
  }

  private async loadExistingDocuments(): Promise<void> {
    try {
      const partitionsPath = this.getPartitionsPath();
      if (!fs.existsSync(partitionsPath)) {
        return;
      }

      let documentsLoaded = 0;
      const partitions = fs.readdirSync(partitionsPath);
      
      for (const partition of partitions) {
        const documentsPath = path.join(partitionsPath, partition, 'documents');
        if (fs.existsSync(documentsPath)) {
          const files = fs.readdirSync(documentsPath);
          
          for (const file of files) {
            if (file.endsWith('.json')) {
              try {
                const documentPath = path.join(documentsPath, file);
                const content = fs.readFileSync(documentPath, 'utf8');
                const doc = JSON.parse(content) as T;
                
                // Extract document ID and add to cache
                const docAny = doc as any;
                const documentId = docAny.id || file.replace('.json', '');
                
                this.cache.set(documentId, doc);
                
                // Rebuild primary index if it wasn't loaded
                if (!this.indexCache.has('_id')) {
                  this.indexCache.set('_id', new Map());
                }
                const primaryIndex = this.indexCache.get('_id')!;
                const sanitizedFileName = file.replace('.json', '');
                primaryIndex.set(documentId, `${partition}/${sanitizedFileName}`);
                
                documentsLoaded++;
              } catch (error) {
                console.error(`Failed to load document ${file}:`, error);
              }
            }
          }
        }
      }
      
      if (documentsLoaded > 0) {
        console.log(`Loaded ${documentsLoaded} existing documents from storage`);
        
        // Save rebuilt index
        await this.saveIndex('_id');
      }
    } catch (error) {
      console.error('Failed to load existing documents:', error);
    }
  }

  private async replayWal(): Promise<void> {
    try {
      const walPath = path.join(this.getWalPath(), 'collection.wal');
      if (!fs.existsSync(walPath)) {
        return;
      }

      const content = fs.readFileSync(walPath, 'utf8');
      const lines = content.trim().split('\n').filter(line => line.trim());
      
      let replayed = 0;
      for (const line of lines) {
        try {
          const entry = JSON.parse(line);
          
          // Verify checksum
          const expectedChecksum = crypto.createHash('md5')
            .update(JSON.stringify({ operation: entry.operation, key: entry.key, value: entry.value }))
            .digest('hex');
          
          if (entry.checksum !== expectedChecksum) {
            console.warn('WAL entry checksum mismatch, skipping:', line);
            continue;
          }

          // Replay operation
          if (entry.operation === 'SET' && entry.value) {
            await this.set(entry.key, entry.value);
          } else if (entry.operation === 'DELETE') {
            await this.delete(entry.key);
          }
          
          replayed++;
        } catch (error) {
          console.warn('Failed to replay WAL entry:', line, error);
        }
      }
      
      if (replayed > 0) {
        console.log(`WAL replay: ${replayed} operations replayed`);
        
        // Clear WAL after successful replay
        fs.unlinkSync(walPath);
      }
    } catch (error) {
      console.error('Failed to replay WAL:', error);
    }
  }

  private async saveIndex(indexName: string): Promise<void> {
    try {
      const index = this.indexCache.get(indexName);
      if (!index) return;

      const indexPath = path.join(this.getGlobalIndexesPath(), `${indexName}.idx`);
      const indexData = Object.fromEntries(index.entries());
      fs.writeFileSync(indexPath, JSON.stringify(indexData, null, 2));
    } catch (error) {
      console.error(`Failed to save index ${indexName}:`, error);
    }
  }

  async set(key: string, value: T): Promise<void> {
    const doc = value as any;
    const partitionKey = this.extractPartitionKey(doc);
    const documentId = doc.id || key;
    const sanitizedDocId = this.sanitizeFilename(documentId);

    // Ensure partition directory exists
    const partitionPath = this.getPartitionPath(partitionKey);
    const documentsPath = path.join(partitionPath, 'documents');
    if (!fs.existsSync(documentsPath)) {
      fs.mkdirSync(documentsPath, { recursive: true });
    }

    // Write document to file
    const documentPath = this.getDocumentPath(partitionKey, documentId);
    fs.writeFileSync(documentPath, JSON.stringify(value, null, 2));

    // Update cache
    this.cache.set(key, value);

    // Update primary index (use original document ID as key, store sanitized location)
    if (!this.indexCache.has('_id')) {
      this.indexCache.set('_id', new Map());
    }
    const primaryIndex = this.indexCache.get('_id')!;
    primaryIndex.set(documentId, `${partitionKey}/${sanitizedDocId}`);

    // Save index asynchronously
    this.saveIndex('_id').catch(console.error);

    // Log to WAL
    await this.writeToWal('SET', key, value);
  }

  async get(key: string): Promise<T | undefined> {
    // Check cache first
    if (this.cache.has(key)) {
      return this.cache.get(key);
    }

    // Look up in primary index
    const primaryIndex = this.indexCache.get('_id');
    if (!primaryIndex) return undefined;

    const location = primaryIndex.get(key);
    if (!location) return undefined;

    const [partitionKey, sanitizedDocId] = location.split('/');
    const partitionPath = this.getPartitionPath(partitionKey);
    const documentPath = path.join(partitionPath, 'documents', `${sanitizedDocId}.json`);

    try {
      if (fs.existsSync(documentPath)) {
        const content = fs.readFileSync(documentPath, 'utf8');
        const value = JSON.parse(content) as T;
        
        // Update cache
        this.cache.set(key, value);
        
        return value;
      }
    } catch (error) {
      console.error(`Failed to read document ${documentPath}:`, error);
    }

    return undefined;
  }

  async delete(key: string): Promise<boolean> {
    // Look up in primary index
    const primaryIndex = this.indexCache.get('_id');
    if (!primaryIndex) return false;

    const location = primaryIndex.get(key);
    if (!location) return false;

    const [partitionKey, sanitizedDocId] = location.split('/');
    const partitionPath = this.getPartitionPath(partitionKey);
    const documentPath = path.join(partitionPath, 'documents', `${sanitizedDocId}.json`);

    try {
      if (fs.existsSync(documentPath)) {
        fs.unlinkSync(documentPath);
        
        // Remove from cache and index
        this.cache.delete(key);
        primaryIndex.delete(key);
        
        // Save index
        await this.saveIndex('_id');
        
        // Log to WAL
        await this.writeToWal('DELETE', key, null);
        
        return true;
      }
    } catch (error) {
      console.error(`Failed to delete document ${documentPath}:`, error);
    }

    return false;
  }

  async has(key: string): Promise<boolean> {
    const primaryIndex = this.indexCache.get('_id');
    return primaryIndex?.has(key) || false;
  }

  async clear(): Promise<void> {
    // Clear cache
    this.cache.clear();
    this.indexCache.clear();

    // Remove all files
    const collectionPath = this.getCollectionPath();
    if (fs.existsSync(collectionPath)) {
      fs.rmSync(collectionPath, { recursive: true, force: true });
    }

    // Recreate directory structure
    await this.ensureDirectoryStructure();
  }

  async size(): Promise<number> {
    const primaryIndex = this.indexCache.get('_id');
    return primaryIndex?.size || 0;
  }

  async *keys(): AsyncIterableIterator<string> {
    const primaryIndex = this.indexCache.get('_id');
    if (primaryIndex) {
      for (const key of primaryIndex.keys()) {
        yield key;
      }
    }
  }

  async *values(): AsyncIterableIterator<T> {
    for await (const key of this.keys()) {
      const value = await this.get(key);
      if (value !== undefined) {
        yield value;
      }
    }
  }

  async *entries(): AsyncIterableIterator<[string, T]> {
    for await (const key of this.keys()) {
      const value = await this.get(key);
      if (value !== undefined) {
        yield [key, value];
      }
    }
  }

  // Sync methods for compatibility
  setSync(key: string, value: T): void {
    this.set(key, value).catch(console.error);
  }

  getSync(key: string): T | undefined {
    return this.cache.get(key);
  }

  deleteSync(key: string): void {
    this.delete(key).catch(console.error);
  }

  hasSync(key: string): boolean {
    const primaryIndex = this.indexCache.get('_id');
    return primaryIndex?.has(key) || false;
  }

  keysSync(): string[] {
    const primaryIndex = this.indexCache.get('_id');
    return primaryIndex ? Array.from(primaryIndex.keys()) : [];
  }

  valuesSync(): T[] {
    return Array.from(this.cache.values());
  }

  entriesSync(): [string, T][] {
    return Array.from(this.cache.entries());
  }

  // Sync version of getAllDocuments for immediate access
  getAllDocumentsSync(): T[] {
    const documents: T[] = [];
    const partitionsPath = this.getPartitionsPath();
    
    if (!fs.existsSync(partitionsPath)) {
      return documents;
    }

    try {
      const partitions = fs.readdirSync(partitionsPath);
      
      for (const partition of partitions) {
        const partitionPath = path.join(partitionsPath, partition, 'documents');
        if (fs.existsSync(partitionPath)) {
          const files = fs.readdirSync(partitionPath);
          for (const file of files) {
            if (file.endsWith('.json')) {
              try {
                const content = fs.readFileSync(path.join(partitionPath, file), 'utf8');
                const doc = JSON.parse(content) as T;
                documents.push(doc);
              } catch (error) {
                console.error(`Failed to read document ${file}:`, error);
              }
            }
          }
        }
      }
    } catch (error) {
      console.error('Failed to read documents:', error);
    }

    return documents;
  }

  // Query methods for partition-based access
  async getPartitionDocuments(partitionKey: string): Promise<T[]> {
    const partitionPath = this.getPartitionPath(partitionKey);
    const documentsPath = path.join(partitionPath, 'documents');
    
    if (!fs.existsSync(documentsPath)) {
      return [];
    }

    const documents: T[] = [];
    const files = fs.readdirSync(documentsPath);
    
    for (const file of files) {
      if (file.endsWith('.json')) {
        try {
          const content = fs.readFileSync(path.join(documentsPath, file), 'utf8');
          const doc = JSON.parse(content) as T;
          documents.push(doc);
        } catch (error) {
          console.error(`Failed to read document ${file}:`, error);
        }
      }
    }

    return documents;
  }

  async getAllDocuments(): Promise<T[]> {
    const documents: T[] = [];
    const partitionsPath = this.getPartitionsPath();
    
    if (!fs.existsSync(partitionsPath)) {
      return documents;
    }

    const partitions = fs.readdirSync(partitionsPath);
    
    for (const partition of partitions) {
      const partitionDocs = await this.getPartitionDocuments(partition);
      documents.push(...partitionDocs);
    }

    return documents;
  }

  private extractPartitionKey(doc: any): string {
    // Extract partition key from document
    // For now, use a simple hash of the document ID to distribute across partitions
    // In a real implementation, this would use the collection's configured partition key path
    const docId = doc.id || 'default';
    
    // Use a simple distribution strategy - first few characters of ID
    if (docId.length > 3) {
      return docId.substring(0, 3);
    }
    
    return 'default';
  }

  private async writeToWal(operation: string, key: string, value: T | null): Promise<void> {
    try {
      const walEntry = {
        timestamp: Date.now(),
        operation,
        key,
        value,
        checksum: crypto.createHash('md5').update(JSON.stringify({ operation, key, value })).digest('hex')
      };

      const walPath = path.join(this.getWalPath(), 'collection.wal');
      fs.appendFileSync(walPath, JSON.stringify(walEntry) + '\n');
    } catch (error) {
      console.error('Failed to write to WAL:', error);
    }
  }

  async close(): Promise<void> {
    // Save all indexes
    for (const indexName of this.indexCache.keys()) {
      await this.saveIndex(indexName);
    }

    this.cache.clear();
    this.indexCache.clear();
    this.isInitialized = false;
  }
}