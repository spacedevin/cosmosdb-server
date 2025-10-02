import * as fs from 'fs';
import * as path from 'path';
import { StorageBackend } from './interfaces';

/**
 * Unified storage backend that handles all data with proper isolation
 * No fragmentation, no duplicate WAL, single source of truth
 */
export class UnifiedStorage<T> implements StorageBackend<T> {
  private cache = new Map<string, T>();
  private dataDir: string;
  private walFile: string;
  private dataFile: string;
  private isInitialized = false;
  private writeQueue: Array<{ key: string; value: T | null; timestamp: number }> = [];
  private isWriting = false;

  constructor(dataDir: string = './cosmos-local-data') {
    this.dataDir = dataDir;
    this.walFile = path.join(dataDir, 'unified.wal');
    this.dataFile = path.join(dataDir, 'unified.json');
  }

  async size(): Promise<number> {
    return this.cache.size;
  }

  async initialize(): Promise<void> {
    if (this.isInitialized) return;

    // Ensure data directory exists
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    // Load existing data
    await this.loadData();
    
    // Replay WAL if it exists
    await this.replayWal();
    
    // Clean up WAL after successful replay
    if (fs.existsSync(this.walFile)) {
      fs.unlinkSync(this.walFile);
    }

    this.isInitialized = true;
    console.log(`Unified storage initialized with ${this.cache.size} entries`);
  }

  private async loadData(): Promise<void> {
    if (fs.existsSync(this.dataFile)) {
      try {
        const content = fs.readFileSync(this.dataFile, 'utf8');
        const data = JSON.parse(content);
        
        // Load all entries into cache
        for (const [key, value] of Object.entries(data)) {
          this.cache.set(key, value as T);
        }
        
        console.log(`Loaded ${this.cache.size} entries from persistent storage`);
      } catch (error) {
        console.warn('Failed to load data file:', error);
      }
    }
  }

  private async replayWal(): Promise<void> {
    if (!fs.existsSync(this.walFile)) return;

    try {
      const content = fs.readFileSync(this.walFile, 'utf8');
      const lines = content.trim().split('\n').filter(line => line.trim());
      
      let replayed = 0;
      for (const line of lines) {
        try {
          const entry = JSON.parse(line);
          if (entry.value === null) {
            this.cache.delete(entry.key);
          } else {
            this.cache.set(entry.key, entry.value);
          }
          replayed++;
        } catch (error) {
          console.warn('Skipping corrupted WAL entry:', line);
        }
      }
      
      console.log(`WAL replay: ${replayed} entries restored`);
    } catch (error) {
      console.warn('Failed to replay WAL:', error);
    }
  }

  async set(key: string, value: T): Promise<void> {
    // Update cache immediately
    this.cache.set(key, value);
    
    // Queue write operation
    this.writeQueue.push({ key, value, timestamp: Date.now() });
    
    // Process write queue
    this.processWrites();
  }

  async get(key: string): Promise<T | undefined> {
    return this.cache.get(key);
  }

  async delete(key: string): Promise<boolean> {
    const existed = this.cache.has(key);
    
    // Remove from cache immediately
    this.cache.delete(key);
    
    // Queue delete operation
    this.writeQueue.push({ key, value: null, timestamp: Date.now() });
    
    // Process write queue
    this.processWrites();
    
    return existed;
  }

  async has(key: string): Promise<boolean> {
    return this.cache.has(key);
  }

  async clear(): Promise<void> {
    this.cache.clear();
    
    // Remove data files
    if (fs.existsSync(this.dataFile)) {
      fs.unlinkSync(this.dataFile);
    }
    if (fs.existsSync(this.walFile)) {
      fs.unlinkSync(this.walFile);
    }
  }

  async *keys(): AsyncIterableIterator<string> {
    for (const key of this.cache.keys()) {
      yield key;
    }
  }

  async *values(): AsyncIterableIterator<T> {
    for (const value of this.cache.values()) {
      yield value;
    }
  }

  async *entries(): AsyncIterableIterator<[string, T]> {
    for (const entry of this.cache.entries()) {
      yield entry;
    }
  }

  private async processWrites(): Promise<void> {
    if (this.isWriting || this.writeQueue.length === 0) return;
    
    this.isWriting = true;
    
    try {
      // Write all queued operations to WAL
      const operations = this.writeQueue.splice(0);
      
      if (operations.length > 0) {
        const walEntries = operations.map(op => JSON.stringify(op)).join('\n') + '\n';
        fs.appendFileSync(this.walFile, walEntries);
        
        // Periodically persist to main data file
        if (Math.random() < 0.1) { // 10% chance to persist
          await this.persistData();
        }
      }
    } catch (error) {
      console.error('Failed to write to WAL:', error);
    } finally {
      this.isWriting = false;
    }
  }

  private async persistData(): Promise<void> {
    try {
      const data = Object.fromEntries(this.cache.entries());
      const content = JSON.stringify(data, null, 2);
      
      // Write to temporary file first
      const tempFile = this.dataFile + '.tmp';
      fs.writeFileSync(tempFile, content);
      
      // Atomic rename
      fs.renameSync(tempFile, this.dataFile);
      
      // Clear WAL after successful persist
      if (fs.existsSync(this.walFile)) {
        fs.unlinkSync(this.walFile);
      }
      
      console.log(`Persisted ${this.cache.size} entries to storage`);
    } catch (error) {
      console.error('Failed to persist data:', error);
    }
  }

  async close(): Promise<void> {
    // Ensure all pending writes are processed
    while (this.writeQueue.length > 0) {
      await this.processWrites();
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    
    // Final persist
    await this.persistData();
    
    this.isInitialized = false;
  }

  // Synchronous methods for compatibility
  setSync(key: string, value: T): void {
    this.cache.set(key, value);
    this.writeQueue.push({ key, value, timestamp: Date.now() });
    // Fire and forget async processing
    this.processWrites().catch(console.error);
  }

  getSync(key: string): T | undefined {
    return this.cache.get(key);
  }

  deleteSync(key: string): void {
    this.cache.delete(key);
    this.writeQueue.push({ key, value: null, timestamp: Date.now() });
    // Fire and forget async processing
    this.processWrites().catch(console.error);
  }

  hasSync(key: string): boolean {
    return this.cache.has(key);
  }

  keysSync(): string[] {
    return Array.from(this.cache.keys());
  }

  valuesSync(): T[] {
    return Array.from(this.cache.values());
  }

  entriesSync(): [string, T][] {
    return Array.from(this.cache.entries());
  }
}