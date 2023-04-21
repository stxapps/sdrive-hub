import { Datastore } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import { FILE_LOG, REVOCATION } from '../const';
import {
  PreconditionFailedError, BadPathError, InvalidInputError, DoesNotExist,
} from '../errors';
import {
  pipelineAsync, logger, dateToUnixTimeSeconds, sample, isObject, isNumber, sleep,
} from '../utils';

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
    contentLength: parseInt(metadata.size),
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
    try {
      const bucket = this.storage.bucket(this.bucket);
      const [exists] = await bucket.exists();
      if (!exists) {
        throw new Error('failed to initialize google cloud storage bucket');
      }
    } catch (err) {
      logger.error(`failed to connect to google cloud storage bucket: ${err}`);
      throw err;
    }
  }

  async deleteEmptyBucket() {
    const files = await this.listFiles({ pathPrefix: '' });
    if (files.entries.length > 0) {
      /* istanbul ignore next */
      throw new Error('Tried deleting non-empty bucket');
    }
    await this.storage.bucket(this.bucket).delete();
  }

  async listAllObjects(prefix, page, pageSize) {
    const opts = {
      prefix: prefix,
      maxResults: pageSize || this.pageSize,
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
    let bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);

    let etag = null, generation = 0, contentLength = 0;

    const stat = await this._performStat(filename, bucketFile);
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

    bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename, { generation });

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
    const fileWriteStream = bucketFile.createWriteStream({
      public: true,
      resumable: this.resumable,
      metadata,
    });

    try {
      await pipelineAsync(args.stream, fileWriteStream);
      logger.debug(`storing ${filename} in bucket ${this.bucket}`);
    } catch (error) {
      if (error.code === 412) {
        throw new PreconditionFailedError(`The provided generation: ${generation} does not match the resource on the server`);
      }
      logger.error(`failed to store ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to store' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }

    const updatedStat = parseFileMetadataStat(bucketFile.metadata)

    const sizeChange = updatedStat.contentLength - contentLength;
    await this.saveFileLog(args.storageTopLevel, args.assoIssAddress, sizeChange);

    return {
      publicURL: `${this.getReadURLPrefix()}${filename}`, etag: updatedStat.etag,
    };
  }

  async performDelete(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }

    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);

    const stat = await this._performStat(filename, bucketFile);
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
      logger.error(`failed to delete ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to delete' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }

    await this.saveFileLog(
      args.storageTopLevel, args.assoIssAddress, -1 * contentLength
    );
  }

  async performRead(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);
    try {
      const [getResult] = await bucketFile.get({ autoCreate: false });
      const statResult = parseFileMetadataStat(getResult.metadata);
      const dataStream = getResult.createReadStream();
      const result = {
        ...statResult,
        exists: true,
        data: dataStream
      };
      return result;
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      logger.error(`failed to read ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to read' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }
  }

  async _performStat(filename, bucketFile) {
    try {
      const [metadataResult] = await bucketFile.getMetadata();
      const result = parseFileMetadataStat(metadataResult);
      return result;
    } catch (error) {
      if (error.code === 404) {
        const result = {
          exists: false,
        };
        return result;
      }
      logger.error(`failed to stat ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to stat ' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }
  }

  async performStat(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);

    const result = await this._performStat(filename, bucketFile);
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
    let bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);

    const stat = await this._performStat(filename, bucketFile);
    if (!stat.exists) throw new DoesNotExist('File does not exist');

    const { etag, generation } = stat;
    this.validateMatchTag(args.ifMatchTag, etag);

    bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename, { generation });

    const newFilename = `${args.storageTopLevel}/${args.newPath}`;
    const newBucketFile = this.storage
      .bucket(this.bucket)
      .file(newFilename);

    try {
      await bucketFile.move(newBucketFile);
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      if (error.code === 412) {
        throw new PreconditionFailedError(`The provided generation: ${generation} does not match the resource on the server`);
      }
      logger.error(`failed to rename ${filename} to ${newFilename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to rename' +
        ` ${filename} to ${newFilename} in bucket ${this.bucket}: ${error}`);
    }

    await this.saveFileLog(args.storageTopLevel, args.assoIssAddress, 0);
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

  validateMatchTag(ifMatchTag, currentETag) {
    if (ifMatchTag && ifMatchTag !== '*') {
      if (ifMatchTag !== currentETag) {
        throw new PreconditionFailedError('The provided ifMatchTag does not match the resource on the server', currentETag);
      }
    }
  }

  async saveFileLog(bucketAddress, assoIssAddress, sizeChange) {
    const logData = [
      { name: 'bucketAddress', value: bucketAddress },
      { name: 'assoIssAddress', value: assoIssAddress },
      { name: 'sizeChange', value: sizeChange, excludeFromIndexes: true },
      { name: 'createDate', value: new Date() },
    ];
    try {
      await this.datastore.save({ key: this.datastore.key([FILE_LOG]), data: logData });
    } catch (error) {
      // Just log. Bucket size will be wrong but need to recal direclty from Storage.
      logger.error(`failed to save FileLog with ${bucketAddress}, ${assoIssAddress}, and ${sizeChange}`);
    }
  }
}

const driver = GcDriver;
export default driver;
