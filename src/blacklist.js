import { LRUCache } from 'lru-cache';

export class BlacklistCache {

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
    if (this.currentCacheEvictions > this.cache.max) {
      console.warn(`Blacklist cache evicted ${this.currentCacheEvictions} entries in the last 10 minutes. Consider increasing 'blacklistCacheSize'.`);
    }
    this.currentCacheEvictions = 0;
  }

  async isBlacklisted(bucketAddress, assoIssAddress) {
    let isBkBltd = /** @type any */(this.cache.get(bucketAddress));
    if (![true, false].includes(isBkBltd)) {
      isBkBltd = await this.driver.performCheckBlacklisted({ keyName: bucketAddress });
      this.cache.set(bucketAddress, isBkBltd);
    }

    let isAiBltd = false;
    if (assoIssAddress) {
      isAiBltd = /** @type any */(this.cache.get(assoIssAddress));
      if (![true, false].includes(isAiBltd)) {
        isAiBltd = await this.driver.performCheckBlacklisted({ keyName: assoIssAddress });
        this.cache.set(assoIssAddress, isAiBltd);
      }
    }

    if (isBkBltd || isAiBltd) return true;
    return false;
  }
}
