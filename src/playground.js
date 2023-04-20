import * as crypto from 'crypto';
import { TokenSigner } from 'jsontokens';
import { ecPairToHexString } from '@stacks/encryption';

const makeAssociationToken = (secretKey, childPublicKey) => {
  const FOUR_MONTH_SECONDS = 60 * 60 * 24 * 31 * 4;
  const publicKeyHex = secretKey.publicKey.toString('hex');
  const salt = crypto.randomBytes(16).toString('hex');
  const payload = {
    childToAssociate: childPublicKey,
    iss: publicKeyHex,
    exp: FOUR_MONTH_SECONDS + (Date.now() / 1000),
    iat: (Date.now() / 1000 | 0),
    gaiaChallenge: String(undefined),
    salt,
  };

  const signerKeyHex = ecPairToHexString(secretKey).slice(0, 64);
  const token = new TokenSigner('ES256K', signerKeyHex).sign(payload);
  return token;
};

const makeAuthPart = (
  secretKey, challengeText, associationToken, hubUrl, scopes, issuedAtDate
) => {

  const FOUR_MONTH_SECONDS = 60 * 60 * 24 * 31 * 4;
  const publicKeyHex = secretKey.publicKey.toString('hex');
  const salt = crypto.randomBytes(16).toString('hex');

  /*if (scopes) {
    validateScopes(scopes);
  }*/

  const payloadIssuedAtDate = issuedAtDate || (Date.now() / 1000 | 0);

  const payload = {
    gaiaChallenge: challengeText,
    iss: publicKeyHex,
    //exp: FOUR_MONTH_SECONDS + (Date.now() / 1000),
    //iat: payloadIssuedAtDate,
    associationToken,
    hubUrl,
    salt,
    //scopes,
  };

  const signerKeyHex = ecPairToHexString(secretKey).slice(0, 64);
  const token = new TokenSigner('ES256K', signerKeyHex).sign(payload);
  return `v1:${token}`;
};
