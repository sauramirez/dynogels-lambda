'use strict';

const Scan = require('./scan');
const Async = require('async');
const NodeUtil = require('util');
const Utils = require('./utils');
const Readable = require('stream').Readable;
const CloneDeep = require('lodash.clonedeep');
const Times = require('lodash.times');

const ParallelScan = module.exports = function (table, serializer, totalSegments) {

  Scan.call(this, table, serializer);
  this.totalSegments = totalSegments;
};

NodeUtil.inherits(ParallelScan, Scan);

ParallelScan.prototype.exec = function (callback) {

  const self = this;
  let streamMode = false;
  const combinedStream = new Readable({ objectMode: true });

  if (!callback) {
    streamMode = true;
    callback = (err) => {

      if (err) {
        combinedStream.emit('error', err);
      }
    };
  }

  const scanFuncs = [];
  Times(self.totalSegments, (segment) => {

    let scn = new Scan(self.table, self.serializer);
    scn.request = CloneDeep(self.request);

    scn = scn.segments(segment, self.totalSegments).loadAll();

    const scanFunc = (cb) => {

      if (streamMode) {
        const stream = scn.exec();

        stream.on('error', cb);

        stream.on('readable', () => {

          const data = stream.read();
          if (data) {
            combinedStream.push(data);
          }
        });

        stream.on('end', cb);
      }
      else {
        return scn.exec(cb);
      }
    };

    scanFuncs.push(scanFunc);
  });

  let started = false;
  const startScans = () => {

    if (started) {
      return;
    }

    started = true;

    Async.parallel(scanFuncs, (err, responses) => {

      if (err) {
        return callback(err);
      }

      combinedStream.push(null);
      return callback(null, Utils.mergeResults(responses, self.table.tableName()));
    });
  };

  if (streamMode) {
    combinedStream._read = startScans;
  }
  else {
    startScans();
  }

  return combinedStream;
};

ParallelScan.prototype.execAsync = function () {
  return new Promise((resolve, reject) => {
    const callback = (err, results) => {
      if (err) {
        reject(err);
      } else {
        resolve(results);
      }
    };
    return this.exec(callback);
  });
};
