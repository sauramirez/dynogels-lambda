'use strict';

const Promises = require('./promises');
const CloneDeep = require('lodash.clonedeep');
const Merge = require('lodash.merge');
const Util = require('util');
const Events = require('events');

const internals = {};

internals.identity = () => {};

const Item = module.exports = function (attrs, table) {

  Events.EventEmitter.call(this);

  this.table = table;

  this.set(attrs || {});
};

Util.inherits(Item, Events.EventEmitter);

Item.prototype.get = function (key) {

  if (key) {
    return this.attrs[key];
  }
  return this.attrs;
};

Item.prototype.set = function (params) {

  this.attrs = Merge({}, this.attrs, params);
  return this;
};

Item.prototype.save = function (callback) {

  const self = this;
  if (!callback) {
    return Promises.wrap(this, this.save);
  }

  self.table.create(this.attrs, (err, item) => {

    if (err) {
      return callback(err);
    }

    self.set(item.attrs);

    return callback(null, item);
  });
};

Item.prototype.update = function (options, callback) {

  const self = this;
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  if (!callback) {
    return Promises.wrap(this, this.update, [options]);
  }

  options = options || {};
  callback = callback || internals.identity;

  self.table.update(this.attrs, options, (err, item) => {

    if (err) {
      return callback(err);
    }

    if (item) {
      self.set(item.attrs);
    }

    return callback(null, item);
  });
};

Item.prototype.destroy = function (options, callback) {

  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  if (!callback) {
    return Promises.wrap(this, this.destroy, [options]);
  }

  options = options || {};
  callback = callback || internals.identity;

  self.table.destroy(this.attrs, options, callback);
};

Item.prototype.toJSON = function () {

  return CloneDeep(this.attrs);
};

Item.prototype.toPlainObject = function () {

  return CloneDeep(this.attrs);
};
