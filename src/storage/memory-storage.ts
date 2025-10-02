import { StorageBackend } from './interfaces';

/**
 * Simple in-memory storage implementation
 */
export class MemoryStorage<T> implements StorageBackend<T> {
  private data = new Map<string, T>();

  async get(key: string): Promise<T | undefined> {
    return this.data.get(key);
  }

  async set(key: string, value: T): Promise<void> {
    this.data.set(key, value);
  }

  async delete(key: string): Promise<boolean> {
    return this.data.delete(key);
  }

  async has(key: string): Promise<boolean> {
    return this.data.has(key);
  }

  async *keys(): AsyncIterableIterator<string> {
    for (const key of this.data.keys()) {
      yield key;
    }
  }

  async clear(): Promise<void> {
    this.data.clear();
  }

  async size(): Promise<number> {
    return this.data.size;
  }

  // Sync helpers for backwards compatibility
  getSync(key: string): T | undefined {
    return this.data.get(key);
  }

  setSync(key: string, value: T): void {
    this.data.set(key, value);
  }

  deleteSync(key: string): boolean {
    return this.data.delete(key);
  }

  hasSync(key: string): boolean {
    return this.data.has(key);
  }

  keysSync(): IterableIterator<string> {
    return this.data.keys();
  }

  sizeSync(): number {
    return this.data.size;
  }

  getUnderlyingMap(): Map<string, T> {
    return this.data;
  }
}
