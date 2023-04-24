import * as ecpair from 'ecpair';
import * as ecc from 'tiny-secp256k1';
import { decodeToken, TokenVerifier } from 'jsontokens';
import { ecPairToAddress } from '@stacks/encryption';

import { ValidationError, AuthTokenTimestampValidationError } from './errors';
import { isNumber } from './utils';

export const LATEST_AUTH_VERSION = 'v1';
const ECPair = ecpair.ECPairFactory(ecc);

const pubkeyHexToECPair = (pubkeyHex) => {
  const pkBuff = Buffer.from(pubkeyHex, 'hex');
  return ECPair.fromPublicKey(pkBuff);
};

export class AuthScopeValues {
  constructor() {
    this.writePrefixes = [];
    this.writePaths = [];
    this.deletePrefixes = [];
    this.deletePaths = [];
    this.writeArchivalPrefixes = [];
    this.writeArchivalPaths = [];
  }
}
AuthScopeValues.parseEntries = (scopes) => {
  const scopeTypes = new AuthScopeValues();
  scopes.forEach(entry => {
    switch (entry.scope) {
      case AuthScopesTypes.putFilePrefix: return scopeTypes.writePrefixes.push(entry.domain);
      case AuthScopesTypes.putFile: return scopeTypes.writePaths.push(entry.domain);
      case AuthScopesTypes.putFileArchival: return scopeTypes.writeArchivalPaths.push(entry.domain);
      case AuthScopesTypes.putFileArchivalPrefix: return scopeTypes.writeArchivalPrefixes.push(entry.domain);
      case AuthScopesTypes.deleteFilePrefix: return scopeTypes.deletePrefixes.push(entry.domain);
      case AuthScopesTypes.deleteFile: return scopeTypes.deletePaths.push(entry.domain);
    }
  });
  return scopeTypes;
};

export const AuthScopesTypes = {
  putFile: 'putFile',
  putFilePrefix: 'putFilePrefix',
  deleteFile: 'deleteFile',
  deleteFilePrefix: 'deleteFilePrefix',
  putFileArchival: 'putFileArchival',
  putFileArchivalPrefix: 'putFileArchivalPrefix',
};

export const AuthScopeTypeArray = Object.values(AuthScopesTypes).filter(val => typeof val === 'string');

export const getTokenPayload = (token) => {
  if (typeof token.payload === 'string') {
    throw new Error('Unexpected token payload type of string');
  }
  return token.payload;
};

export const decodeTokenForPayload = (opts) => {
  try {
    return getTokenPayload(decodeToken(opts.encodedToken));
  } catch (e) {
    throw new ValidationError(opts.validationErrorMsg);
  }
};

export class V1Authentication {

  constructor(token) {
    this.token = token;
    this.authPayload = null;
    this.assoPayload = null;
    this.assoIssAddress = null;
  }

  checkAssociationToken(token, bearerAddress, options) {
    // a JWT can have an `associationToken` that was signed by one of the
    // whitelisted addresses on this server.  This method checks a given
    // associationToken and verifies that it authorizes the "outer"
    // JWT's address (`bearerAddress`)
    const payload = decodeTokenForPayload({
      encodedToken: token,
      validationErrorMsg: 'checkAssociationToken: Failed to decode association token',
    });

    // publicKey (the issuer of the association token)
    // will be the whitelisted address (i.e. the identity address)
    const publicKey = payload.iss;
    const childPublicKey = payload.childToAssociate;
    const expiresAt = payload.exp;

    if (!publicKey) {
      throw new ValidationError('Must provide `iss` claim in association JWT.');
    }

    if (!childPublicKey) {
      throw new ValidationError('Must provide `childToAssociate` claim in association JWT.');
    }

    if (!expiresAt) {
      throw new ValidationError('Must provide `exp` claim in association JWT.');
    }

    // check for revocations
    if (
      options &&
      options.oldestValidTokenTimestamp &&
      options.oldestValidTokenTimestamp > 0
    ) {
      const tokenIssuedAtDate = payload.iat;
      const oldestValidTokenTimestamp = options.oldestValidTokenTimestamp;
      if (!isNumber(tokenIssuedAtDate)) {
        const message = `Gaia bucket requires auth token issued after ${oldestValidTokenTimestamp}` + ' but this token has no creation timestamp. This token may have been revoked by the user.';
        throw new AuthTokenTimestampValidationError(message, oldestValidTokenTimestamp);
      }
      if (tokenIssuedAtDate < options.oldestValidTokenTimestamp) {
        const message = `Gaia bucket requires auth token issued after ${oldestValidTokenTimestamp}` + ` but this token was issued ${tokenIssuedAtDate}.` + ' This token may have been revoked by the user.';
        throw new AuthTokenTimestampValidationError(message, oldestValidTokenTimestamp);
      }
    }

    let verified;
    try {
      verified = new TokenVerifier('ES256K', publicKey).verify(token);
    } catch (err) {
      throw new ValidationError('Failed to verify association JWT: invalid issuer');
    }
    if (!verified) {
      throw new ValidationError('Failed to verify association JWT: invalid issuer');
    }

    if (expiresAt < (Date.now() / 1000)) {
      throw new ValidationError(`Expired association token: expire time of ${expiresAt} (secs since epoch)`);
    }

    // the bearer of the association token must have authorized the bearer
    const childAddress = ecPairToAddress(pubkeyHexToECPair(childPublicKey));
    if (childAddress !== bearerAddress) {
      throw new ValidationError(`Association token child key ${childPublicKey} does not match ${bearerAddress}`);
    }

    this.assoPayload = payload;
    this.assoIssAddress = ecPairToAddress(pubkeyHexToECPair(publicKey));
  }

  parseAuthScopes() {
    const scopes = this.getAuthenticationScopes();
    return AuthScopeValues.parseEntries(scopes);
  }

  /*
   * Get the authentication token's association token's scopes.
   * Does not validate the authentication token or the association token
   * (do that with isAuthenticationValid first).
   *
   * Returns the scopes, if there are any given.
   * Returns [] if there is no association token, or if the association token has no scopes
   */
  getAuthenticationScopes() {
    const payload = this.authPayload; // if !this.authPayload, let it error!

    if (!payload['scopes']) {
      // not given
      return [];
    }

    // unambiguously convert to AuthScope
    const scopes = (payload.scopes).map((s) => {
      const r = {
        scope: String(s.scope),
        domain: String(s.domain)
      };
      return r;
    });

    return scopes;
  }

  /*
   * Determine if the authentication token is valid:
   * * must have signed the given `challengeText`
   * * must not be expired
   * * if it contains an associationToken, then the associationToken must
   *   authorize the given address.
   *
   * Returns the address that signed off on this token, which will be
   * checked against the server's whitelist.
   * * If this token has an associationToken, then the signing address
   *   is the address that signed the associationToken.
   * * Otherwise, the signing address is the given address.
   *
   * this throws a ValidationError if the authentication is invalid
   */
  isAuthenticationValid(address, challengeTexts, options) {
    const payload = decodeTokenForPayload({
      encodedToken: this.token,
      validationErrorMsg: 'isAuthenticationValid: Failed to decode authentication JWT',
    });

    const publicKey = payload.iss;
    const gaiaChallenge = payload.gaiaChallenge;
    const scopes = payload.scopes;

    if (!publicKey) {
      throw new ValidationError('Must provide `iss` claim in JWT.');
    }

    // check for revocations
    if (
      'iat' in payload &&
      options &&
      options.oldestValidTokenTimestamp &&
      options.oldestValidTokenTimestamp > 0
    ) {
      const tokenIssuedAtDate = payload.iat;
      const oldestValidTokenTimestamp = options.oldestValidTokenTimestamp;
      if (!isNumber(tokenIssuedAtDate)) {
        const message = `Gaia bucket requires auth token issued after ${oldestValidTokenTimestamp}` + ' but this token has no creation timestamp. This token may have been revoked by the user.';
        throw new AuthTokenTimestampValidationError(message, oldestValidTokenTimestamp);
      }
      if (tokenIssuedAtDate < options.oldestValidTokenTimestamp) {
        const message = `Gaia bucket requires auth token issued after ${oldestValidTokenTimestamp}` + ` but this token was issued ${tokenIssuedAtDate}.` + ' This token may have been revoked by the user.';
        throw new AuthTokenTimestampValidationError(message, oldestValidTokenTimestamp);
      }
    }

    const issuerAddress = ecPairToAddress(pubkeyHexToECPair(publicKey));

    if (issuerAddress !== address) {
      throw new ValidationError('Address not allowed to write on this path');
    }

    if (options && options.requireCorrectHubUrl) {
      let claimedHub = payload.hubUrl || payload.gaiaHubUrl;
      if (!claimedHub) {
        throw new ValidationError('Authentication must provide a claimed hub. You may need to update stacks.js.');
      }
      if (claimedHub.endsWith('/')) {
        claimedHub = claimedHub.slice(0, -1);
      }
      const validHubUrls = options.validHubUrls;
      if (!validHubUrls) {
        throw new ValidationError('Configuration error on the gaia hub. validHubUrls must be supplied.');
      }
      if (!validHubUrls.includes(claimedHub)) {
        throw new ValidationError(`Auth token's claimed hub url '${claimedHub}' not found` + ` in this hubs set: ${JSON.stringify(validHubUrls)}`);
      }
    }

    if (scopes) {
      validateScopes(scopes);
    }

    let verified;
    try {
      verified = new TokenVerifier('ES256K', publicKey).verify(this.token);
    } catch (err) {
      throw new ValidationError('Failed to verify supplied authentication JWT');
    }
    if (!verified) {
      throw new ValidationError('Failed to verify supplied authentication JWT');
    }

    if (!challengeTexts.includes(gaiaChallenge)) {
      throw new ValidationError(`Invalid gaiaChallenge text in supplied JWT: "${gaiaChallenge}"` + ` not found in ${JSON.stringify(challengeTexts)}`);
    }

    const expiresAt = payload.exp;
    if (expiresAt && expiresAt < (Date.now() / 1000)) {
      throw new ValidationError(`Expired authentication token: expire time of ${expiresAt} (secs since epoch)`);
    }

    if ('associationToken' in payload && payload.associationToken) {
      this.checkAssociationToken(payload.associationToken, address, options);
    } else {
      // Storing wallet-config.js or profile.json,
      //   the authToken doesn't have association key as no app yet!
      //throw new ValidationError('Must provide `associationToken` in JWT.');
    }

    this.authPayload = payload;
  }
}
V1Authentication.fromAuthPart = (authPart) => {
  if (!authPart.startsWith('v1:')) {
    throw new ValidationError('Authorization header should start with v1:');
  }
  const token = authPart.slice('v1:'.length);
  return new V1Authentication(token);
};

export const getChallengeText = (myURL) => {
  const header = 'gaiahub';
  const allowedSpan = '0';
  const myChallenge = 'blockstack_storage_please_sign';
  return JSON.stringify([header, allowedSpan, myURL, myChallenge]);
};

export const parseAuthHeader = (authHeader) => {
  if (!authHeader || !authHeader.toLowerCase().startsWith('bearer')) {
    throw new ValidationError('Failed to parse authentication header.');
  }
  const authPart = authHeader.slice('bearer '.length);
  const versionIndex = authPart.indexOf(':');
  if (versionIndex < 0) {
    // default to legacy authorization header
    throw new ValidationError('Not support legacy authentication');
  } else {
    const version = authPart.slice(0, versionIndex);
    if (version === 'v1') {
      return V1Authentication.fromAuthPart(authPart);
    } else {
      throw new ValidationError(`Unknown authentication header version: ${version}`);
    }
  }
};

export const validateAuthorizationHeader = (
  authHeader, serverName, address, requireCorrectHubUrl, validHubUrls,
  oldestValidTokenTimestamp, whitelist,
) => {
  const authObject = parseAuthHeader(authHeader);

  const serverNameHubUrl = `https://${serverName}`;
  if (!validHubUrls) {
    validHubUrls = [serverNameHubUrl];
  } else if (!validHubUrls.includes(serverNameHubUrl)) {
    validHubUrls.push(serverNameHubUrl);
  }

  const challengeTexts = [];
  challengeTexts.push(getChallengeText(serverName));

  authObject.isAuthenticationValid(
    address,
    challengeTexts,
    { validHubUrls, requireCorrectHubUrl, oldestValidTokenTimestamp }
  );

  const signingAddress = authObject.assoIssAddress || address;
  if (whitelist && !(whitelist.includes(signingAddress))) {
    throw new ValidationError(`Address ${signingAddress} not authorized for writes`);
  }

  return authObject;
};

/*
 * Validate authentication scopes.  They must be well-formed,
 * and there can't be too many of them.
 * Return true if valid.
 * Throw ValidationError on error
 */
export const validateScopes = (scopes) => {
  if (scopes.length > 8) {
    throw new ValidationError('Too many authentication scopes');
  }

  for (let i = 0; i < scopes.length; i++) {
    const scope = scopes[i];

    // valid scope?
    const found = AuthScopeTypeArray.find((s) => (s === scope.scope));
    if (!found) {
      throw new ValidationError(`Unrecognized scope ${scope.scope}`);
    }
  }

  return true;
};
