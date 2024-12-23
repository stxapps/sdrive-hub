import { validateAuthorizationHeader } from './authentication';
import {
  ValidationError, DoesNotExist, PayloadTooLargeError, PreconditionFailedError,
  InvalidInputError,
} from './errors';
import { AuthTimestampCache } from './revocations';
import { BlacklistCache } from './blacklist';
import { PUT_FILE, DELETE_FILE, LIST_FILES, PERFORM_FILES } from './const';
import {
  generateUniqueID, bytesToMegabytes, megabytesToBytes, monitorStreamProgress, isString,
  isObject,
} from './utils';

export class HubServer {

  constructor(driver, config) {
    this.driver = driver;
    this.config = config;
    this.whitelist = config.whitelist;
    this.serverName = config.serverName;
    this.validHubUrls = config.validHubUrls;
    this.readURL = config.readURL;
    this.requireCorrectHubUrl = config.requireCorrectHubUrl || false;
    this.authTimestampCache = new AuthTimestampCache(
      driver, config.authTimestampCacheSize
    );
    this.blacklistCache = new BlacklistCache(
      driver, config.blacklistCacheSize
    );
    this.maxFileUploadSizeMB = (config.maxFileUploadSize || 20);
    this.maxFileUploadSizeBytes = megabytesToBytes(this.maxFileUploadSizeMB);
  }

  async handleAuthBump(address, oldestValidTimestamp, requestHeaders) {
    this.validate(address, requestHeaders, null);
    await this.authTimestampCache.setAuthTimestamp(address, oldestValidTimestamp);
  }

  validate(address, requestHeaders, oldestValidTokenTimestamp) {
    const authObject = validateAuthorizationHeader(
      requestHeaders.authorization,
      this.serverName,
      address,
      this.requireCorrectHubUrl,
      this.validHubUrls,
      oldestValidTokenTimestamp,
      this.whitelist,
    );
    return authObject;
  }

  async handleListFiles(address, page, pageSize, stat, requestHeaders) {
    const [oldestValidTokenTimestamp, isBkBltd] = await Promise.all([
      this.authTimestampCache.getAuthTimestamp(address),
      this.blacklistCache.isBlacklisted(address, LIST_FILES),
    ]);
    if (isBkBltd) {
      throw new ValidationError(`Address ${address} is on the not authorized list`);
    }

    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );
    if (authObject.assoIssAddress !== null) {
      // Not check for now. Can avoid easily by not providing assoIssAddress.
      /*const isAiBltd = await this.blacklistCache.isBlacklisted(
        authObject.assoIssAddress, LIST_FILES
      );
      if (isAiBltd) {
        throw new ValidationError(`assoIssAddress ${authObject.assoIssAddress} is on the not authorized list`);
      }*/
    }

    const scopes = authObject.parseAuthScopes();
    const isArchivalRestricted = this.isArchivalRestricted(scopes);

    const listFilesArgs = {
      pathPrefix: address + '/', // to exclude ${address}-auth from revocation
      page: page,
      pageSize: pageSize,
    };

    let listFileResult;
    if (stat) {
      listFileResult = await this.driver.listFilesStat(listFilesArgs);
    } else {
      listFileResult = await this.driver.listFiles(listFilesArgs);
    }

    // Filter historical files from results.
    if (isArchivalRestricted && listFileResult.entries.length > 0) {
      if (stat) {
        listFileResult.entries = listFileResult
          .entries
          .filter(entry => !this.isHistoricalFile(entry.name));
      } else {
        listFileResult.entries = listFileResult
          .entries
          .filter(entry => !this.isHistoricalFile(entry));
      }

      // Detect empty page due to all files being historical files.
      if (listFileResult.entries.length === 0 && listFileResult.page) {
        // Insert a null marker entry to indicate that there are more results
        // even though the entry array is empty.
        listFileResult.entries.push(null);
      }
    }

    return listFileResult;
  }

  getReadURLPrefix() {
    if (this.readURL) {
      return this.readURL;
    } else {
      return this.driver.getReadURLPrefix();
    }
  }

  getFileName(filePath) {
    const pathParts = filePath.split('/');
    const fileName = pathParts[pathParts.length - 1];
    return fileName;
  }

  getHistoricalFileName(filePath) {
    const fileName = this.getFileName(filePath);
    const filePathPrefix = filePath.slice(0, filePath.length - fileName.length);
    const historicalName = `.history.${Date.now()}.${generateUniqueID()}.${fileName}`;
    const historicalPath = `${filePathPrefix}${historicalName}`;
    return historicalPath;
  }

  isHistoricalFile(filePath) {
    const fileName = this.getFileName(filePath);
    const isHistoricalFile = fileName.startsWith('.history.');
    return isHistoricalFile;
  }

  async handleDelete(address, path, requestHeaders) {
    const [oldestValidTokenTimestamp, isBkBltd] = await Promise.all([
      this.authTimestampCache.getAuthTimestamp(address),
      this.blacklistCache.isBlacklisted(address, DELETE_FILE),
    ]);
    if (isBkBltd) {
      throw new ValidationError(`Address ${address} is on the not authorized list`);
    }

    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );
    if (authObject.assoIssAddress !== null) {
      // Not check for now. Can avoid easily by not providing assoIssAddress.
      /*const isAiBltd = await this.blacklistCache.isBlacklisted(
        authObject.assoIssAddress, DELETE_FILE
      );
      if (isAiBltd) {
        throw new ValidationError(`assoIssAddress ${authObject.assoIssAddress} is on the not authorized list`);
      }*/
    }

    // can the caller delete? if so, in what paths?
    const scopes = authObject.parseAuthScopes();
    const isArchivalRestricted = this.checkArchivalRestrictions(address, path, scopes);

    if (scopes.deletePrefixes.length > 0 || scopes.deletePaths.length > 0) {
      // we're limited to a set of prefixes and paths.
      // does the given path match any prefixes?
      let match = !!scopes.deletePrefixes.find((p) => (path.startsWith(p)));

      if (!match) {
        // check for exact paths
        match = !!scopes.deletePaths.find((p) => (path === p));
      }

      if (!match) {
        // not authorized to write to this path
        throw new ValidationError(`Address ${address} not authorized to delete from ${path} by scopes`);
      }
    }

    const ifMatchTag = requestHeaders['if-match'];
    const ifNoneMatchTag = requestHeaders['if-none-match'];
    if (ifNoneMatchTag) {
      throw new PreconditionFailedError('Not support if-none-match for file deletion.');
    }

    let result;
    if (isArchivalRestricted) {
      // if archival restricted then just rename the canonical file
      //   to the historical file.
      const historicalPath = this.getHistoricalFileName(path);
      result = await this.driver.performRename({
        path: path,
        storageTopLevel: address,
        newPath: historicalPath,
        ifMatchTag: ifMatchTag,
        assoIssAddress: authObject.assoIssAddress,
      });
    } else {
      result = await this.driver.performDelete({
        storageTopLevel: address,
        path,
        ifMatchTag: ifMatchTag,
        assoIssAddress: authObject.assoIssAddress,
      });
    }

    const backupPaths = [], fileLogs = [];
    if (isObject(result)) {
      backupPaths.push(...result.backupPaths);
      fileLogs.push(...result.fileLogs);
    }
    await this.driver.addTaskToQueue(backupPaths, fileLogs);
  }

  async handleRequest(address, path, requestHeaders, stream) {
    const [oldestValidTokenTimestamp, isBkBltd] = await Promise.all([
      this.authTimestampCache.getAuthTimestamp(address),
      this.blacklistCache.isBlacklisted(address, PUT_FILE),
    ]);
    if (isBkBltd) {
      throw new ValidationError(`Address ${address} is on the not authorized list`);
    }

    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );
    if (authObject.assoIssAddress !== null) {
      // Not check for now. Can avoid easily by not providing assoIssAddress.
      /*const isAiBltd = await this.blacklistCache.isBlacklisted(
        authObject.assoIssAddress, PUT_FILE
      );
      if (isAiBltd) {
        throw new ValidationError(`assoIssAddress ${authObject.assoIssAddress} is on the not authorized list`);
      }*/
    }

    // can the caller write? if so, in what paths?
    const scopes = authObject.parseAuthScopes();
    const isArchivalRestricted = this.checkArchivalRestrictions(address, path, scopes);

    if (scopes.writePrefixes.length > 0 || scopes.writePaths.length > 0) {
      // we're limited to a set of prefixes and paths.
      // does the given path match any prefixes?
      let match = !!scopes.writePrefixes.find((p) => (path.startsWith(p)));

      if (!match) {
        // check for exact paths
        match = !!scopes.writePaths.find((p) => (path === p));
      }

      if (!match) {
        // not authorized to write to this path
        throw new ValidationError(`Address ${address} not authorized to write to ${path} by scopes`);
      }
    }

    const ifMatchTag = requestHeaders['if-match'];
    const ifNoneMatchTag = requestHeaders['if-none-match'];
    // only one match-tag header should be set
    if (ifMatchTag && ifNoneMatchTag) {
      throw new PreconditionFailedError('Request should not contain both if-match and if-none-match headers');
    }
    // only support using if-none-match for file creation, values that aren't the wildcard are prohibited
    if (ifNoneMatchTag && ifNoneMatchTag !== '*') {
      throw new PreconditionFailedError('Misuse of the if-none-match header. Expected to be * on write requests.');
    }

    let contentType = requestHeaders['content-type'];
    if (contentType === null || contentType === undefined) {
      contentType = 'application/octet-stream';
    }

    const contentLengthHeader = requestHeaders['content-length'];
    const contentLengthBytes = parseInt(contentLengthHeader, 10);
    const isLengthFinite = Number.isFinite(contentLengthBytes) && contentLengthBytes > 0;

    // If a valid content-length is specified check to immediately return error
    if (isLengthFinite && contentLengthBytes > this.maxFileUploadSizeBytes) {
      const errMsg = (
        `Max file upload size is ${this.maxFileUploadSizeMB} megabytes. ` +
        `Rejected Content-Length of ${bytesToMegabytes(contentLengthBytes, 4)} megabytes`
      );
      throw new PayloadTooLargeError(errMsg);
    }

    let rnResult;
    if (isArchivalRestricted) {
      const historicalPath = this.getHistoricalFileName(path);
      try {
        rnResult = await this.driver.performRename({
          path: path,
          storageTopLevel: address,
          newPath: historicalPath,
          ifMatchTag: ifMatchTag,
          assoIssAddress: authObject.assoIssAddress,
        });
      } catch (error) {
        if (error instanceof DoesNotExist) {
          console.debug(
            '404 on putFileArchival rename attempt -- usually this is okay and ' +
            'only indicates that this is the first time the file was written: ' +
            `${address}/${path}`
          );
        } else {
          throw error;
        }
      }
    }

    // Use the client reported content-length if available, otherwise fallback to the
    // max configured length.
    const maxContentLength = (
      Number.isFinite(contentLengthBytes) && contentLengthBytes > 0
        ? contentLengthBytes : this.maxFileUploadSizeBytes
    );

    // Create a PassThrough stream to monitor streaming chunk sizes.
    const { monitoredStream, pipelinePromise } = monitorStreamProgress(
      stream,
      totalBytes => {
        if (totalBytes > maxContentLength) {
          const errMsg = (
            `Max file upload size is ${this.maxFileUploadSizeMB} megabytes. ` +
            `Rejected POST body stream of ${bytesToMegabytes(totalBytes, 4)} megabytes`
          );
          // Log error -- this situation is indicative of a malformed client request
          // where the reported Content-Size is less than the upload size.
          console.warn(`${errMsg}, address: ${address}`);

          // Destroy the request stream -- cancels reading from the client
          // and cancels uploading to the storage driver.
          const error = new PayloadTooLargeError(errMsg);
          stream.destroy(error);
          throw error;
        }
      }
    );

    const writeCommand = {
      storageTopLevel: address,
      path,
      content: monitoredStream,
      contentType,
      contentLength: contentLengthBytes,
      ifMatchTag: ifMatchTag,
      ifNoneMatchTag: ifNoneMatchTag,
      assoIssAddress: authObject.assoIssAddress,
    };
    const [wResult] = await Promise.all([
      this.driver.performWrite(writeCommand), pipelinePromise,
    ]);
    const writeResponse = this.fixWriteResponse(wResult.result)

    const backupPaths = [], fileLogs = [];
    if (isObject(rnResult)) {
      backupPaths.push(...rnResult.backupPaths);
      fileLogs.push(...rnResult.fileLogs);
    }
    backupPaths.push(...wResult.backupPaths);
    fileLogs.push(...wResult.fileLogs);
    await this.driver.addTaskToQueue(backupPaths, fileLogs);

    return writeResponse;
  }

  isArchivalRestricted(scopes) {
    return (
      scopes.writeArchivalPaths.length > 0 || scopes.writeArchivalPrefixes.length > 0
    );
  }

  checkArchivalRestrictions(address, path, scopes) {
    const isArchivalRestricted = this.isArchivalRestricted(scopes);
    if (isArchivalRestricted) {
      // we're limited to a set of prefixes and paths.
      // does the given path match any prefixes?
      let match = !!scopes.writeArchivalPrefixes.find((p) => (path.startsWith(p)));

      if (!match) {
        // check for exact paths
        match = !!scopes.writeArchivalPaths.find((p) => (path === p));
      }

      if (!match) {
        // not authorized to write to this path
        throw new ValidationError(`Address ${address} not authorized to modify ${path} by scopes`);
      }
    }
    return isArchivalRestricted;
  }

  fixWriteResponse(writeResponse) {
    const readURL = writeResponse.publicURL;
    const driverPrefix = this.driver.getReadURLPrefix();
    const readURLPrefix = this.getReadURLPrefix();
    if (readURLPrefix !== driverPrefix && readURL.startsWith(driverPrefix)) {
      const postFix = readURL.slice(driverPrefix.length);
      const fixedWriteResponse = {
        ...writeResponse, publicURL: `${readURLPrefix}${postFix}`,
      };
      return fixedWriteResponse;
    }

    return writeResponse;
  }

  async _handlePerformFile(address, assoIssAddress, scopes, data) {
    const { id, type, path } = data;

    const isArchivalRestricted = this.checkArchivalRestrictions(address, path, scopes);

    if (type === PUT_FILE) {
      const isBkBltd = await this.blacklistCache.isBlacklisted(address, PUT_FILE);
      if (isBkBltd) {
        throw new ValidationError(`Address ${address} is on the not authorized list`);
      }
      if (assoIssAddress !== null) {
        // Not check for now. Can avoid easily by not providing assoIssAddress.
        /*const isAiBltd = await this.blacklistCache.isBlacklisted(
          assoIssAddress, PUT_FILE
        );
        if (isAiBltd) {
          throw new ValidationError(`assoIssAddress ${assoIssAddress} is on the not authorized list`);
        }*/
      }

      if (scopes.writePrefixes.length > 0 || scopes.writePaths.length > 0) {
        // we're limited to a set of prefixes and paths.
        // does the given path match any prefixes?
        let match = !!scopes.writePrefixes.find((p) => (path.startsWith(p)));

        if (!match) {
          // check for exact paths
          match = !!scopes.writePaths.find((p) => (path === p));
        }

        if (!match) {
          // not authorized to write to this path
          throw new ValidationError(`Address ${address} not authorized to write to ${path} by scopes`);
        }
      }

      let { contentType, content } = data;
      if (isString(content)) {
        if (!isString(contentType)) contentType = 'text/plain';
      } else if (isObject(content)) {
        if (!isString(contentType)) contentType = 'application/json';
        content = JSON.stringify(content);
      } else {
        throw new InvalidInputError(`Invalid data.content: ${content}`);
      }

      const contentLengthBytes = Buffer.byteLength(content, 'utf8');
      if (contentLengthBytes > this.maxFileUploadSizeBytes) {
        const errMsg = (
          `Max file upload size is ${this.maxFileUploadSizeMB} megabytes. ` +
          `Rejected data.content of ${bytesToMegabytes(contentLengthBytes, 4)} megabytes`
        );
        throw new PayloadTooLargeError(errMsg);
      }

      let rnResult;
      if (isArchivalRestricted) {
        const historicalPath = this.getHistoricalFileName(path);
        try {
          rnResult = await this.driver.performRename({
            path: path,
            storageTopLevel: address,
            newPath: historicalPath,
            ifMatchTag: null,
            assoIssAddress: assoIssAddress,
          });
        } catch (error) {
          if (error instanceof DoesNotExist) {
            console.debug(
              '404 on putFileArchival rename attempt -- usually this is okay and ' +
              'only indicates that this is the first time the file was written: ' +
              `${address}/${path}`
            );
          } else {
            throw error;
          }
        }
      }

      const writeCommand = {
        storageTopLevel: address,
        path,
        content,
        contentType,
        contentLength: contentLengthBytes,
        ifMatchTag: null,
        ifNoneMatchTag: null,
        assoIssAddress: assoIssAddress,
      };
      const wResult = await this.driver.performWrite(writeCommand);
      let writeResponse = this.fixWriteResponse(wResult.result);
      writeResponse = { ...writeResponse, success: true, id };

      const backupPaths = [], fileLogs = [];
      if (isObject(rnResult)) {
        backupPaths.push(...rnResult.backupPaths);
        fileLogs.push(...rnResult.fileLogs);
      }
      backupPaths.push(...wResult.backupPaths);
      fileLogs.push(...wResult.fileLogs);

      return { result: writeResponse, backupPaths, fileLogs };
    }

    if (type === DELETE_FILE) {
      const isBkBltd = await this.blacklistCache.isBlacklisted(address, DELETE_FILE);
      if (isBkBltd) {
        throw new ValidationError(`Address ${address} is on the not authorized list`);
      }
      if (assoIssAddress !== null) {
        // Not check for now. Can avoid easily by not providing assoIssAddress.
        /*const isAiBltd = await this.blacklistCache.isBlacklisted(
          assoIssAddress, DELETE_FILE
        );
        if (isAiBltd) {
          throw new ValidationError(`assoIssAddress ${assoIssAddress} is on the not authorized list`);
        }*/
      }

      if (scopes.deletePrefixes.length > 0 || scopes.deletePaths.length > 0) {
        // we're limited to a set of prefixes and paths.
        // does the given path match any prefixes?
        let match = !!scopes.deletePrefixes.find((p) => (path.startsWith(p)));

        if (!match) {
          // check for exact paths
          match = !!scopes.deletePaths.find((p) => (path === p));
        }

        if (!match) {
          // not authorized to write to this path
          throw new ValidationError(`Address ${address} not authorized to delete from ${path} by scopes`);
        }
      }

      let result;
      if (isArchivalRestricted) {
        // if archival restricted then just rename the canonical file
        //   to the historical file.
        const historicalPath = this.getHistoricalFileName(path);
        result = await this.driver.performRename({
          path: path,
          storageTopLevel: address,
          newPath: historicalPath,
          ifMatchTag: null,
          assoIssAddress: assoIssAddress,
        });
      } else {
        const { doIgnoreDoesNotExistError } = data;
        try {
          result = await this.driver.performDelete({
            storageTopLevel: address,
            path,
            ifMatchTag: null,
            assoIssAddress: assoIssAddress,
          });
        } catch (error) {
          if (!doIgnoreDoesNotExistError || !(error instanceof DoesNotExist)) {
            throw error;
          }
        }
      }

      const backupPaths = [], fileLogs = [];
      if (isObject(result)) {
        backupPaths.push(...result.backupPaths);
        fileLogs.push(...result.fileLogs);
      }

      return { result: { success: true, id }, backupPaths, fileLogs };
    }

    throw new InvalidInputError(`Invalid data.type: ${data.type}`);
  }

  async _handlePerformFiles(address, assoIssAddress, scopes, data) {
    const results = [];

    if (Array.isArray(data.values) && [true, false].includes(data.isSequential)) {
      if (data.isSequential) {
        for (const value of data.values) {
          const pResults = await this._handlePerformFiles(
            address, assoIssAddress, scopes, value
          );
          results.push(...pResults);
          if (pResults.some(pResult => !pResult.result.success)) break;
        }
      } else {
        const nItems = 10;
        for (let i = 0; i < data.values.length; i += nItems) {
          const selectedValues = data.values.slice(i, i + nItems);
          const aResults = await Promise.all(selectedValues.map(value => {
            return this._handlePerformFiles(
              address, assoIssAddress, scopes, value
            );
          }));
          for (const pResults of aResults) {
            results.push(...pResults);
          }
        }
      }
    } else if (isString(data.id) && isString(data.type) && isString(data.path)) {
      try {
        const pResult = await this._handlePerformFile(
          address, assoIssAddress, scopes, data
        );
        results.push(pResult);
      } catch (error) {
        const result = {
          error: error.toString().slice(0, 999), success: false, id: data.id,
        };
        results.push({ result, backupPaths: [], fileLogs: [] });
      }
    } else {
      console.log('In handlePerformFiles, invalid data:', data);
    }

    return results;
  }

  async handlePerformFiles(address, requestBody, requestHeaders) {
    const [oldestValidTokenTimestamp, isBkBltd] = await Promise.all([
      this.authTimestampCache.getAuthTimestamp(address),
      this.blacklistCache.isBlacklisted(address, PERFORM_FILES),
    ]);
    if (isBkBltd) {
      throw new ValidationError(`Address ${address} is on the not authorized list`);
    }

    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );
    if (authObject.assoIssAddress !== null) {
      // Not check for now. Can avoid easily by not providing assoIssAddress.
      /*const isAiBltd = await this.blacklistCache.isBlacklisted(
        authObject.assoIssAddress, PERFORM_FILES
      );
      if (isAiBltd) {
        throw new ValidationError(`assoIssAddress ${authObject.assoIssAddress} is on the not authorized list`);
      }*/
    }

    const scopes = authObject.parseAuthScopes();

    const pResults = await this._handlePerformFiles(
      address, authObject.assoIssAddress, scopes, requestBody
    );

    const response = [], backupPaths = [], fileLogs = [];
    for (const pfResult of pResults) {
      response.push(pfResult.result);
      backupPaths.push(...pfResult.backupPaths);
      fileLogs.push(...pfResult.fileLogs);
    }
    await this.driver.addTaskToQueue(backupPaths, fileLogs);

    return response;
  }
}
