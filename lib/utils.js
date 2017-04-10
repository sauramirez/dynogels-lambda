'use strict';

const Readable = require('stream').Readable;
const Async = require('async');
const AWS = require('aws-sdk');
const IsString = require('lodash.isstring');
const IsEmpty = require('lodash.isempty');
const Omit = require('lodash.omit');
const OmitBy = require('lodash.omitby');
const Reduce = require('lodash.reduce');

const utils = module.exports;

utils.omitNulls = (data) => {

  return OmitBy(data, (value) => {

    return value === null ||
      value === undefined ||
      (Array.isArray(value) && IsEmpty(value)) ||
      (IsString(value) && IsEmpty(value));
  });
};

utils.mergeResults = (responses, tableName) => {

  const result = {
    Items: [],
    ConsumedCapacity: {
      CapacityUnits: 0,
      TableName: tableName
    },
    Count: 0,
    ScannedCount: 0
  };

  const merged = Reduce(responses, (memo, resp) => {

    if (!resp) {
      return memo;
    }

    memo.Count += resp.Count || 0;
    memo.ScannedCount += resp.ScannedCount || 0;

    if (resp.ConsumedCapacity) {
      memo.ConsumedCapacity.CapacityUnits += resp.ConsumedCapacity.CapacityUnits || 0;
    }

    if (resp.Items) {
      memo.Items = memo.Items.concat(resp.Items);
    }

    if (resp.LastEvaluatedKey) {
      memo.LastEvaluatedKey = resp.LastEvaluatedKey;
    }

    return memo;
  }, result);

  if (merged.ConsumedCapacity.CapacityUnits === 0) {
    delete merged.ConsumedCapacity;
  }

  if (merged.ScannedCount === 0) {
    delete merged.ScannedCount;
  }

  return merged;
};

utils.paginatedRequest = (self, runRequestFunc, callback) => {
  // if callback isn't passed switch to stream
  if (!callback) {
    return utils.streamRequest(self, runRequestFunc);
  }

  let lastEvaluatedKey = null;
  const responses = [];
  let retry = false;

  const doFunc = (cb) => {

    if (lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    runRequestFunc(self.buildRequest(), (err, resp) => {

      if (err && err.retryable) {
        retry = true;
        return setImmediate(cb);
      }
      else if (err) {
        retry = false;
        return setImmediate(cb, err);
      }

      retry = false;
      lastEvaluatedKey = resp.LastEvaluatedKey;

      responses.push(resp);

      return setImmediate(cb);
    });
  };

  const testFunc = () => (self.options.loadAll && lastEvaluatedKey) || retry;

  const resulsFunc = (err) => {

    if (err) {
      return callback(err);
    }
    return callback(null, utils.mergeResults(responses, self.table.tableName()));
  };
  return Async.doWhilst(doFunc, testFunc, resulsFunc);
};


utils.streamRequest = (self, runRequestFunc) => {

  let lastEvaluatedKey = null;
  let performRequest = true;
  const stream = new Readable({ objectMode: true });
  const startRead = () => {

    if (!performRequest) {
      return;
    }
    if (lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }
    runRequestFunc(self.buildRequest(), (err, resp) => {

      if (err && err.retryable) {
        return setTimeout(startRead, 1000);
      }
      else if (err) {
        return stream.emit('error', err);
      }
      lastEvaluatedKey = resp.LastEvaluatedKey;
      if (!self.options.loadAll || !lastEvaluatedKey) {
        performRequest = false;
      }
      stream.push(resp);
      if (!self.options.loadAll || !lastEvaluatedKey) {
        stream.push(null);
      }
    });
  };

  stream._read = () => {

    startRead();
  };

  return stream;
};

utils.omitPrimaryKeys = (schema, params) => Omit(params, schema.hashKey, schema.rangeKey);

utils.strToBin = (value) => {

  if (typeof value !== 'string') {
    const StrConversionError = 'Need to pass in string primitive to be converted to binary.';
    throw new Error(StrConversionError);
  }

  if (AWS.util.isBrowser()) {
    const len = value.length;
    const bin = new Uint8Array(new ArrayBuffer(len));
    for (let i = 0; i < len; ++i) {
      bin[i] = value.charCodeAt(i);
    }
    return bin;
  }
  return AWS.util.Buffer(value);
};
