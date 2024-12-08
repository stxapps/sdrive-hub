import * as crypto from 'crypto';
import { TokenSigner } from 'jsontokens';
import { getPublicKeyFromPrivate, /*publicKeyToAddress*/ } from '@stacks/encryption';

import { Datastore } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';
import { BLACKLIST } from './const';

const makeAssociationToken = (privateKey, childPublicKey) => {
  const FOUR_MONTH_SECONDS = 60 * 60 * 24 * 31 * 4;
  const iss = getPublicKeyFromPrivate(privateKey);
  const salt = crypto.randomBytes(16).toString('hex');
  const payload = {
    childToAssociate: childPublicKey,
    iss,
    exp: FOUR_MONTH_SECONDS + (Date.now() / 1000),
    iat: (Date.now() / 1000 | 0),
    salt,
  };

  const token = new TokenSigner('ES256K', privateKey).sign(payload);
  return token;
};

const makeAuthToken = (
  privateKey, challengeText, associationToken, hubUrl, scopes, issuedAtDate
) => {

  //const FOUR_MONTH_SECONDS = 60 * 60 * 24 * 31 * 4;
  const iss = getPublicKeyFromPrivate(privateKey);
  const salt = crypto.randomBytes(16).toString('hex');

  /*if (scopes) {
    validateScopes(scopes);
  }*/

  //const payloadIssuedAtDate = issuedAtDate || (Date.now() / 1000 | 0);

  const payload = {
    gaiaChallenge: challengeText,
    iss,
    //exp: FOUR_MONTH_SECONDS + (Date.now() / 1000),
    //iat: payloadIssuedAtDate,
    associationToken,
    hubUrl,
    salt,
  };
  if (scopes) payload.scopes = scopes;

  const token = new TokenSigner('ES256K', privateKey).sign(payload);
  return `v1:${token}`;
};

const playAuthToken = async () => {

  const salt = crypto.randomBytes(16).toString('hex');
  console.log(salt);
  return;

  const dataPrivateKey = '';
  const appPrivateKey = '';
  if (dataPrivateKey.length !== 64 || appPrivateKey.length !== 64) {
    throw new Error('Wrong! Need to slice(0, 64)?');
  }

  const appPublicKey = getPublicKeyFromPrivate(appPrivateKey);
  console.log('appPublicKey', appPublicKey);

  //const appAddress = publicKeyToAddress(appPublicKey);
  //console.log('appAddress', appAddress);

  const assoToken = makeAssociationToken(dataPrivateKey, appPublicKey);

  const challengeText = "[\"gaiahub\",\"0\",\"hub.stacksdrive.com\",\"blockstack_storage_please_sign\"]";
  const scopes = [{ scope: 'putFileArchivalPrefix', domain: 'test' }];

  const authToken = makeAuthToken(
    appPrivateKey, challengeText, assoToken, 'https://hub.stacksdrive.com', scopes
  );
  console.log('authToken', authToken);
};
//playAuthToken();

const play0 = async () => {
  const storage = new Storage();

  const bucket = storage.bucket('gcp-public-data-arco-era5'); //gcp-public-data-sentinel-2

  let pageToken = null;
  for (let i = 0; i < 10; i++) {
    const opts = {
      prefix: 'raw/ERA5GRIB/HRES/Month/', //tiles/08
      maxResults: 1000,
      pageToken: pageToken,
    };
    const [files, nextQuery] = await bucket.getFiles(opts);
    if (nextQuery && nextQuery.pageToken) pageToken = nextQuery.pageToken;
    console.log(files.length);
    console.log(files[0].name);
  }
};

const play1 = async () => {
  const storage = new Storage();

  const bucket = storage.bucket('sdrive-001.appspot.com');

  const file = bucket.file('1JNsK64gpFc63a3RVrwGXebXZRxo2zJULn/test2.json');
  console.log(file.metadata);

  await file.getMetadata();

  console.log(file.metadata);

  /*const res = await bucket.file('1JNsK64gpFc63a3RVrwGXebXZRxo2zJULn/test2.json').delete();
  const what = res[0].body;
  console.log(what);*/
};
//play1();

const play2 = async () => {
  const res = await fetch(
    "https://api.hiro.so/v1/addresses/stacks/SP1V7W5N0Y9KKY3QG73GQH0NZWJ65VBNJY93HKATD"
  );
  const result = await res.json();
  console.log(result);
};
//play2();

const play3 = async () => {
  const datastore = new Datastore();
  const key = datastore.key([BLACKLIST, '1NEGXF4cg7wqsnkGXXDxdJgUGnRP14ofLF']);
  const data = [
    { name: 'type', value: 0 },
    { name: 'createDate', value: new Date() },
  ];
  await datastore.save({ key, data: data });
};
play3();
