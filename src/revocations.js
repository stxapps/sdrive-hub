import { LRUCache } from 'lru-cache';

export class AuthTimestampCache {

  constructor(driver, maxCacheSize) {
    this.currentCacheEvictions = 0;
    this.cache = new LRUCache({
      max: maxCacheSize,
      noDisposeOnSet: true,
      dispose: () => {
        this.currentCacheEvictions++
      },
      ttl: 15 * 60 * 1000,
      ttlResolution: 60 * 1000,
    });
    this.driver = driver;

    const tenMinutes = 1000 * 60 * 10;
    this.setupCacheEvictionLogger(tenMinutes);
  }

  setupCacheEvictionLogger(timerInterval) {
    const evictionLogTimeout = setInterval(
      () => this.handleCacheEvictions(), timerInterval
    );
    evictionLogTimeout.unref();
  }

  handleCacheEvictions() {
    if (this.currentCacheEvictions > 0) {
      console.warn(`Gaia authentication token timestamp cache evicted ${this.currentCacheEvictions} entries in the last 10 minutes. Consider increasing 'authTimestampCacheSize'.`);
      this.currentCacheEvictions = 0;
    }
  }

  async getAuthTimestamp(bucketAddress) {
    // First perform fast check if auth number exists in cache.
    let authTimestamp = this.cache.get(bucketAddress);
    if (authTimestamp) {
      return authTimestamp;
    }

    // Nothing in cache, perform slower driver read.
    authTimestamp = await this.driver.performReadAuthTimestamp({ bucketAddress });

    // Recheck cache for a larger timestamp to avoid race conditions from slow storage.
    const cachedTimestamp = this.cache.get(bucketAddress);
    if (cachedTimestamp && cachedTimestamp > authTimestamp) {
      return cachedTimestamp;
    }

    // Cache result for fast lookup later.
    this.cache.set(bucketAddress, authTimestamp);

    return authTimestamp;
  }

  async setAuthTimestamp(bucketAddress, timestamp) {
    // Recheck cache for a larger timestamp to avoid race conditions from slow storage.
    let cachedTimestamp = this.cache.get(bucketAddress);
    if (cachedTimestamp && cachedTimestamp > timestamp) {
      return;
    }

    await this.driver.performWriteAuthTimestamp({ bucketAddress, timestamp });

    // In a race condition, use the newest timestamp.
    cachedTimestamp = this.cache.get(bucketAddress);
    if (cachedTimestamp && cachedTimestamp > timestamp) {
      return;
    }

    this.cache.set(bucketAddress, timestamp);
  }
}
