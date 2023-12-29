import { Datastore } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import {
  FILE_LOG, REVOCATION, BLACKLIST, PUT_FILE, DELETE_FILE, MOVE_FILE_PUT_STEP,
  MOVE_FILE_DEL_STEP,
} from '../const';
import {
  PreconditionFailedError, BadPathError, InvalidInputError, DoesNotExist,
} from '../errors';
import { dateToUnixTimeSeconds, sample, isObject, isNumber, sleep } from '../utils';

const isPathValid = (path) => {
  // for now, only disallow double dots.
  return !path.includes('..');
};

const formatETagFromMD5 = (md5Hash) => {
  const hex = Buffer.from(md5Hash, 'base64').toString('hex');
  const formatted = `"${hex}"`;
  return formatted;
};

const parseFileMetadataStat = (metadata) => {
  const lastModified = dateToUnixTimeSeconds(new Date(metadata.updated));
  const result = {
    exists: true,
    etag: formatETagFromMD5(metadata.md5Hash),
    contentType: metadata.contentType,
    contentLength: parseInt(metadata.size, 10),
    lastModifiedDate: lastModified,
    generation: metadata.generation,
  };
  return result;
};

class GcDriver {

  constructor(config) {
    this.datastore = new Datastore();
    this.storage = new Storage();
    this.bucket = config.bucket;
    this.backupBucket = config.backupBucket;
    this.pageSize = config.pageSize ? config.pageSize : 100;
    this.cacheControl = config.cacheControl;
    this.initPromise = this.createIfNeeded();
    this.resumable = config.resumable || false;
  }

  ensureInitialized() {
    return this.initPromise;
  }

  dispose() {
    return Promise.resolve();
  }

  getReadURLPrefix() {
    return `https://storage.googleapis.com/${this.bucket}/`;
  }

  async createIfNeeded() {
    const bucket = this.storage.bucket(this.bucket);
    const [exists] = await bucket.exists();
    if (!exists) {
      throw new Error('failed to initialize google cloud storage bucket');
    }
  }

  async deleteEmptyBucket() {
    const files = await this.listFiles({ pathPrefix: '' });
    if (files.entries.length > 0) {
      throw new Error('Tried deleting non-empty bucket');
    }
    await this.storage.bucket(this.bucket).delete();
  }

  async listAllObjects(prefix, page, pageSize) {
    pageSize = parseInt(pageSize, 10);
    if (!Number.isFinite(pageSize) || pageSize > this.pageSize || pageSize <= 0) {
      pageSize = this.pageSize;
    }
    const opts = {
      prefix: prefix,
      maxResults: pageSize,
      pageToken: page || undefined,
    };

    const getFilesResult = await new Promise((resolve, reject) => {
      this.storage
        .bucket(this.bucket)
        .getFiles(opts, (err, files, nextQuery) => {
          if (err) {
            reject(err);
          } else {
            resolve({ files, nextQuery });
          }
        });
    });
    const fileEntries = getFilesResult.files.map(file => {
      return {
        name: file.name.slice(prefix.length),
        file: file,
      };
    });
    const result = {
      entries: fileEntries,
      page: (getFilesResult.nextQuery && getFilesResult.nextQuery.pageToken) || null,
    };
    return result;
  }

  async listFiles(args) {
    const listResult = await this.listAllObjects(
      args.pathPrefix, args.page, args.pageSize
    );
    const result = {
      page: listResult.page,
      entries: listResult.entries.map(file => file.name),
    };
    return result;
  }

  async listFilesStat(args) {
    const listResult = await this.listAllObjects(
      args.pathPrefix, args.page, args.pageSize
    );
    const result = {
      page: listResult.page,
      entries: listResult.entries.map(entry => {
        const statResult = parseFileMetadataStat(entry.file.metadata);
        const entryResult = {
          ...statResult,
          name: entry.name,
          exists: true,
        }
        return entryResult;
      }),
    };
    return result;
  }

  async performWrite(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    if (args.contentType && args.contentType.length > 1024) {
      throw new InvalidInputError('Invalid content-type');
    }

    const filename = `${args.storageTopLevel}/${args.path}`;
    let bucketFile = this.storage.bucket(this.bucket).file(filename);

    let etag = null, generation = 0, contentLength = 0;

    const stat = await this._performStat(bucketFile);
    if (stat.exists) {
      const { etag: etg, generation: gnt, contentLength: ctl } = stat;
      [etag, generation, contentLength] = [etg, gnt, ctl];
    }
    this.validateMatchTag(args.ifMatchTag, etag);
    if (args.ifNoneMatchTag && args.ifNoneMatchTag === '*') {
      // only proceed with writing file if the file does not already exist
      if (stat.exists) {
        throw new PreconditionFailedError('The entity you are trying to create already exists');
      }
    }

    bucketFile = this.storage.bucket(this.bucket).file(filename, { generation });

    /* There is some overhead when using a resumable upload that can cause
       noticeable performance degradation while uploading a series of small
       files. When uploading files less than 10MB, it is recommended that
       the resumable feature is disabled.
       For details see https://github.com/googleapis/nodejs-storage/issues/312 */
    const metadata = {};
    metadata.contentType = args.contentType;
    if (this.cacheControl) {
      metadata.cacheControl = this.cacheControl;
    }

    try {
      await bucketFile.save(
        args.content, { public: true, resumable: this.resumable, metadata }
      );
    } catch (error) {
      if (error.code === 412) {
        throw new PreconditionFailedError(`The provided generation: ${generation} does not match the resource on the server`);
      }

      throw error;
    }

    await this.performBackup(bucketFile.name);

    const udtdStat = parseFileMetadataStat(bucketFile.metadata)
    const udtdCtl = udtdStat.contentLength;
    const sizeChange = udtdCtl - contentLength;
    await this.saveFileLog(
      bucketFile.name, args.assoIssAddress, PUT_FILE, udtdCtl, sizeChange
    );

    return {
      publicURL: `${this.getReadURLPrefix()}${bucketFile.name}`, etag: udtdStat.etag,
    };
  }

  async performDelete(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }

    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage.bucket(this.bucket).file(filename);

    const stat = await this._performStat(bucketFile);
    if (!stat.exists) throw new DoesNotExist('File does not exist');

    const { etag, generation, contentLength } = stat;
    this.validateMatchTag(args.ifMatchTag, etag);

    try {
      await bucketFile.delete({ ifGenerationMatch: generation });
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      if (error.code === 412) {
        throw new PreconditionFailedError(`The provided generation: ${generation} does not match the resource on the server`);
      }

      throw error;
    }

    await this.saveFileLog(
      bucketFile.name, args.assoIssAddress, DELETE_FILE, 0, -1 * contentLength
    );
  }

  async performRead(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }

    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage.bucket(this.bucket).file(filename);

    try {
      const [getResult] = await bucketFile.get({ autoCreate: false });
      const statResult = parseFileMetadataStat(getResult.metadata);
      const dataStream = getResult.createReadStream();
      const result = { ...statResult, exists: true, data: dataStream };
      return result;
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }

      throw error;
    }
  }

  async _performStat(bucketFile) {
    try {
      const [metadataResult] = await bucketFile.getMetadata();
      const result = parseFileMetadataStat(metadataResult);
      return result;
    } catch (error) {
      if (error.code === 404) {
        const result = { exists: false };
        return result;
      }

      throw error;
    }
  }

  async performStat(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage.bucket(this.bucket).file(filename);

    const result = await this._performStat(bucketFile);
    return result;
  }

  async performRename(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid original path');
    }
    if (!isPathValid(args.newPath)) {
      throw new BadPathError('Invalid new path');
    }

    const filename = `${args.storageTopLevel}/${args.path}`;
    let bucketFile = this.storage.bucket(this.bucket).file(filename);

    const stat = await this._performStat(bucketFile);
    if (!stat.exists) throw new DoesNotExist('File does not exist');

    const { etag, generation, contentLength } = stat;
    this.validateMatchTag(args.ifMatchTag, etag);

    bucketFile = this.storage.bucket(this.bucket).file(filename, { generation });

    const newFilename = `${args.storageTopLevel}/${args.newPath}`;
    const newBucketFile = this.storage.bucket(this.bucket).file(newFilename);

    try {
      await bucketFile.move(newBucketFile);
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      if (error.code === 412) {
        throw new PreconditionFailedError(`The provided generation: ${generation} does not match the resource on the server`);
      }

      throw error;
    }

    await this.performBackup(newBucketFile.name);

    const udtdStat = parseFileMetadataStat(newBucketFile.metadata);
    const udtdCtl = udtdStat.contentLength;
    await this.saveFileLog(
      bucketFile.name, args.assoIssAddress, MOVE_FILE_DEL_STEP, 0, -1 * contentLength
    );
    await this.saveFileLog(
      newBucketFile.name, args.assoIssAddress, MOVE_FILE_PUT_STEP, udtdCtl, udtdCtl
    );
  }

  async performWriteAuthTimestamp(args) {
    const { bucketAddress, timestamp } = args;

    const date = new Date();

    const key = this.datastore.key([REVOCATION, bucketAddress]);
    const data = [
      { name: 'timestamp', value: timestamp, excludeFromIndexes: true },
      { name: 'createDate', value: date },
      { name: 'updateDate', value: date },
    ];
    const entity = { key, data };

    const nTries = 2;
    for (let currentTry = 1; currentTry <= nTries; currentTry++) {
      const transaction = this.datastore.transaction();
      try {
        await transaction.run();

        const [oEy] = await transaction.get(key);
        if (isObject(oEy) && isNumber(oEy.timestamp)) {
          if (oEy.timestamp < timestamp) {
            entity.data[1].value = oEy.createDate;
            transaction.save({ key, data });
          }
        } else {
          transaction.save(entity);
        }

        await transaction.commit();
        return;
      } catch (error) {
        await transaction.rollback();

        if (currentTry < nTries) await sleep(sample([100, 200, 280, 350]));
        else throw error;
      }
    }
  }

  async performReadAuthTimestamp(args) {
    const { bucketAddress } = args;

    const key = this.datastore.key([REVOCATION, bucketAddress]);
    const [entity] = await this.datastore.get(key);

    let timestamp = 0;
    if (isObject(entity) && isNumber(entity.timestamp)) {
      timestamp = entity.timestamp;
    }

    return timestamp;
  }

  async performCheckBlacklisted(args) {
    const { keyName } = args;

    const key = this.datastore.key([BLACKLIST, keyName]);
    const [entity] = await this.datastore.get(key);

    if (isObject(entity)) return true;
    return false;
  }

  validateMatchTag(ifMatchTag, currentETag) {
    if (ifMatchTag && ifMatchTag !== '*') {
      if (ifMatchTag !== currentETag) {
        throw new PreconditionFailedError('The provided ifMatchTag does not match the resource on the server', currentETag);
      }
    }
  }

  async performBackup(fileName) {
    try {
      const bucketFile = this.storage.bucket(this.bucket).file(fileName);
      const backupBucket = this.storage.bucket(this.backupBucket)
      await bucketFile.copy(backupBucket, { predefinedAcl: 'private' });
    } catch (error) {
      // Just log. Need to manually copy directly from Storage.
      console.error(`Error performBackup: ${fileName}`, error);
    }
  }

  async saveFileLog(path, assoIssAddress, action, size, sizeChange) {
    const logData = [
      { name: 'path', value: path, excludeFromIndexes: true },
      { name: 'assoIssAddress', value: assoIssAddress, excludeFromIndexes: true },
      { name: 'action', value: action, excludeFromIndexes: true },
      { name: 'size', value: size, excludeFromIndexes: true },
      { name: 'sizeChange', value: sizeChange, excludeFromIndexes: true },
      { name: 'createDate', value: new Date() },
    ];

    try {
      await this.datastore.save({ key: this.datastore.key([FILE_LOG]), data: logData });
    } catch (error) {
      // Just log. Bucket size will be wrong, need to recal direclty from Storage.
      console.error(`Error saveFileLog: ${path}`, error);
    }
  }
}

const driver = GcDriver;
export default driver;
