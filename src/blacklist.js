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

  async isBlacklisted(address) {
    let isBltd = /** @type any */(this.cache.get(address));
    if ([true, false].includes(isBltd)) return isBltd;

    isBltd = await this.driver.performCheckBlacklisted({ keyName: address });
    this.cache.set(address, isBltd);

    return isBltd;
  }
}
