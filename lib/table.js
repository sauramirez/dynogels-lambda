'use strict';

const Item = require('./item');
const Query = require('./query');
const Scan = require('./scan');
const EventEmitter = require('events').EventEmitter;
const Async = require('async');
const Utils = require('./utils');
const ParallelScan = require('./parallelScan');
const Promises = require('./promises');
const expressions = require('./expressions');
const Compact = require('lodash.compact');
const Find = require('lodash.find');
const Foreach = require('lodash.foreach');
const Has = require('lodash.has');
const Includes = require('lodash.includes');
const IsBoolean = require('lodash.isboolean');
const IsEmpty = require('lodash.isempty');
const IsPlainObject = require('lodash.isplainobject');
const IsObject = require('lodash.isobject');
const IsString = require('lodash.isstring');
const Keys = require('lodash.keys');
const Map = require('lodash.map');
const Merge = require('lodash.merge');
const Omit = require('lodash.omit');
const OmitBy = require('lodash.omitby');
const PickBy = require('lodash.pickby');
const Set = require('lodash.set');
const Get = require('lodash.get');
const Reduce = require('lodash.reduce');
const Values = require('lodash.values');

const internals = {};

const Table = module.exports = function (name, schema, serializer, docClient, logger) {

  this.config = { name };
  this.schema = schema;
  this.serializer = serializer;
  this.docClient = docClient;
  this.log = logger;

  this._before = new EventEmitter();
  this.before = this._before.on.bind(this._before);

  this._after = new EventEmitter();
  this.after = this._after.on.bind(this._after);
};

Table.prototype.initItem = function (attrs) {
  const self = this;

  if (self.itemFactory) {
    return new self.itemFactory(attrs);
  } else {
    return new Item(attrs, self);
  }
};

Table.prototype.tableName = function () {
  if (this.schema.tableName) {
    if (this.schema.tableName.constructor === Function) {
      return this.schema.tableName.call(this);
    }
    return this.schema.tableName;
  }
  return this.config.name;
};

Table.prototype.sendRequest = function (method, params, callback) {

  const self = this;

  let driver;
  if (self.docClient[method] && self.docClient[method].constructor === Function) {
    driver = self.docClient;
  } else if (self.docClient.service[method].constructor === Function) {
    driver = self.docClient.service;
  }

  const startTime = Date.now();

  self.log.info({ params: params }, 'dynogels %s request', method.toUpperCase());
  driver[method].call(driver, params, (err, data) => {
    const elapsed = Date.now() - startTime;

    if (err) {
      self.log.warn({ err: err }, 'dynogels %s error', method.toUpperCase());
      return callback(err);
    } else {
      self.log.info({ data: data }, 'dynogels %s response - %sms', method.toUpperCase(), elapsed);
      return callback(null, data);
    }
  });
};

Table.prototype.get = function (hashKey, rangeKey, options, callback) {
  const self = this;

  if (IsPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  if (!callback) {
    return Promises.wrap(this, this.get, [hashKey, rangeKey, options]);
  }

  let params = {
    TableName: self.tableName(),
    Key: self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  params = Merge({}, params, options);

  self.sendRequest('get', params, (err, data) => {
    if (err) {
      return callback(err);
    }

    let item = null;
    if (data.Item) {
      item = self.initItem(self.serializer.deserializeItem(data.Item));
    }

    return callback(null, item);
  });
};

internals.callBeforeHooks = (table, name, startFun, callback) => {
  const listeners = table._before.listeners(name);

  return Async.waterfall([startFun].concat(listeners), callback);
};

Table.prototype.create = function (item, options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  if (!callback) {
    return Promises.wrap(this, this.create, [item, options]);
  }
  options = options || {};

  if (Array.isArray(item)) {
    Async.map(item, (data, callback) => internals.createItem(self, data, options, callback), callback);
  } else {
    return internals.createItem(self, item, options, callback);
  }
};

internals.createItem = (table, item, options, callback) => {
  const self = table;

  const start = callback => {
    const data = self.schema.applyDefaults(item);

    const paramName = IsString(self.schema.createdAt) ? self.schema.createdAt : 'createdAt';

    if (self.schema.timestamps && self.schema.createdAt !== false && !Has(data, paramName)) {
      data[paramName] = new Date().toISOString();
    }

    return callback(null, data);
  };

  internals.callBeforeHooks(self, 'create', start, (err, data) => {
    if (err) {
      return callback(err);
    }

    const result = self.schema.validate(data);

    if (result.error) {
      result.error.message = `${result.error.message} on ${self.tableName()}`;
      return callback(result.error);
    }

    const attrs = Utils.omitNulls(data);

    let params = {
      TableName: self.tableName(),
      Item: self.serializer.serializeItem(self.schema, attrs)
    };

    if (options.expected) {
      internals.addConditionExpression(params, options.expected);
      options = Omit(options, 'expected');
    }

    if (options.overwrite === false) {
      const expected = Reduce(Compact([self.schema.hashKey, self.schema.rangeKey]), (res, key) => {

        Set(res, `${key}.<>`, Get(params.Item, key));
        return res;
      }, {});

      internals.addConditionExpression(params, expected);
    }

    options = Omit(options, 'overwrite'); // remove overwrite flag regardless if true or false

    params = Merge({}, params, options);

    self.sendRequest('put', params, err => {
      if (err) {
        return callback(err);
      }

      const item = self.initItem(attrs);
      self._after.emit('create', item);

      return callback(null, item);
    });
  });
};

internals.updateExpressions = (schema, data, options) => {
  const exp = expressions.serializeUpdateExpression(schema, data);

  if (options.UpdateExpression) {
    const parsed = expressions.parse(options.UpdateExpression);

    exp.expressions = Reduce(parsed, (result, val, key) => {
      if (!IsEmpty(val)) {
        result[key] = result[key].concat(val);
      }

      return result;
    }, exp.expressions);
  }

  if (IsPlainObject(options.ExpressionAttributeValues)) {
    exp.values = Merge({}, exp.values, options.ExpressionAttributeValues);
  }

  if (IsPlainObject(options.ExpressionAttributeNames)) {
    exp.attributeNames = Merge({}, exp.attributeNames, options.ExpressionAttributeNames);
  }

  return Merge({}, {
    ExpressionAttributeValues: exp.values,
    ExpressionAttributeNames: exp.attributeNames,
    UpdateExpression: expressions.stringify(exp.expressions),
  });
};

internals.validateItemFragment = (item, schema) => {
  const result = {};
  const error = {};

  // get the list of attributes to remove
  const removeAttributes = PickBy(item, (x) => x === null);

  // get the list of attributes whose value is an object
  const setOperationValues = PickBy(item, (i) => IsPlainObject(i) && (i.$add || i.$del));

  // get the list of attributes to modify
  const updateAttributes = Omit(
    item,
    Object.keys(removeAttributes).concat(Object.keys(setOperationValues))
  );

  // check attribute removals for .required() schema violation
  const removalValidation = schema.validate(
    {},
    { abortEarly: false }
  );

  if (removalValidation.error) {
    const errors = PickBy(
      removalValidation.error.details,
      e => IsEqual(e.type, 'any.required')
      && Object.prototype.hasOwnProperty.call(removeAttributes, e.path)
    );
    if (!IsEmpty(errors)) {
      error.remove = errors;
      result.error = error;
    }
  }

  // check attribute updates match the schema
  const updateValidation = schema.validate(
    updateAttributes,
    { abortEarly: false }
  );

  if (updateValidation.error) {
    const errors = OmitBy(
      updateValidation.error.details,
      e => IsEqual(e.type, 'any.required')
    );
    if (!IsEmpty(errors)) {
      error.update = errors;
      result.error = error;
    }
  }

  return result;
};

Table.prototype.update = function (item, options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  if (!callback) {
    return Promises.wrap(this, this.update, [item, options]);
  }
  options = options || {};

  const schemaValidation = internals.validateItemFragment(item, self.schema);
  if (schemaValidation.error) {
    return callback(_.assign(new Error(`Schema validation error while updating item in table ${self.tableName()}: ${JSON.stringify(schemaValidation.error)}`), {
      name: 'DynogelsUpdateError',
      detail: schemaValidation.error
    }));
  }

  const start = callback => {
    const paramName = IsString(self.schema.updatedAt) ? self.schema.updatedAt : 'updatedAt';

    if (self.schema.timestamps && self.schema.updatedAt !== false && !Has(item, paramName)) {
      item[paramName] = new Date().toISOString();
    }

    return callback(null, item);
  };

  internals.callBeforeHooks(self, 'update', start, (err, data) => {
    if (err) {
      return callback(err);
    }

    const hashKey = data[self.schema.hashKey];
    let rangeKey = data[self.schema.rangeKey];

    if (rangeKey === undefined) {
      rangeKey = null;
    }

    let params = {
      TableName: self.tableName(),
      Key: self.serializer.buildKey(hashKey, rangeKey, self.schema),
      ReturnValues: 'ALL_NEW'
    };

    let exp = null;
    try {
      exp = internals.updateExpressions(self.schema, data, options);
    } catch (e) {
      return callback(e);
    }

    params = _.assign(params, exp);

    if (options.expected) {
      internals.addConditionExpression(params, options.expected);
    }

    params = OmitBy(Merge(params, options), IsEmpty);

    self.sendRequest('update', params, (err, data) => {
      if (err) {
        return callback(err);
      }

      let result = null;
      if (data.Attributes) {
        result = self.initItem(self.serializer.deserializeItem(data.Attributes));
      }

      self._after.emit('update', result);
      return callback(null, result);
    });
  });
};

internals.addConditionExpression = (params, expectedConditions) => {
  Foreach(expectedConditions, (val, key) => {
    let operator;
    let expectedValue = null;

    const existingValueKeys = Keys(params.ExpressionAttributeValues);

    if (IsObject(val) && IsBoolean(val.Exists) && val.Exists === true) {
      operator = 'attribute_exists';
    }
    else if (IsObject(val) && IsBoolean(val.Exists) && val.Exists === false) {
      operator = 'attribute_not_exists';
    }
    else if (IsObject(val) && Has(val, '<>')) {
      operator = '<>';
      expectedValue = Get(val, '<>');
    }
    else {
      operator = '=';
      expectedValue = val;
    }

    const condition = expressions.buildFilterExpression(key, operator, existingValueKeys, expectedValue, null);
    params.ExpressionAttributeNames = Merge({}, condition.attributeNames, params.ExpressionAttributeNames);
    params.ExpressionAttributeValues = Merge({}, condition.attributeValues, params.ExpressionAttributeValues);

    if (IsString(params.ConditionExpression)) {
      params.ConditionExpression = `${params.ConditionExpression} AND (${condition.statement})`;
    }
    else {
      params.ConditionExpression = `(${condition.statement})`;
    }
  });
};

Table.prototype.destroy = function (hashKey, rangeKey, options, callback) {
  const self = this;

  if (IsPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (IsPlainObject(rangeKey) && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  if (!callback) {
    return Promises.wrap(this, this.destroy, [hashKey, rangeKey, options]);
  }
  options = options || {};

  if (IsPlainObject(hashKey)) {
    rangeKey = hashKey[self.schema.rangeKey];

    if (rangeKey === undefined) {
      rangeKey = null;
    }

    hashKey = hashKey[self.schema.hashKey];
  }

  let params = {
    TableName: self.tableName(),
    Key: self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  if (options.expected) {
    internals.addConditionExpression(params, options.expected);

    delete options.expected;
  }

  params = Merge({}, params, options);

  self.sendRequest('delete', params, (err, data) => {
    if (err) {
      return callback(err);
    }

    let item = null;
    if (data.Attributes) {
      item = self.initItem(self.serializer.deserializeItem(data.Attributes));
    }

    self._after.emit('destroy', item);
    return callback(null, item);
  });
};

Table.prototype.query = function (hashKey) {
  const self = this;

  return new Query(hashKey, self, self.serializer);
};

Table.prototype.scan = function () {
  const self = this;

  return new Scan(self, self.serializer);
};

Table.prototype.parallelScan = function (totalSegments) {
  const self = this;

  return new ParallelScan(self, self.serializer, totalSegments);
};


internals.deserializeItems = (table, callback) => (err, data) => {
  if (err) {
    return callback(err);
  }

  const result = {};
  if (data.Items) {
    result.Items = Map(data.Items, i => table.initItem(table.serializer.deserializeItem(i)));

    delete data.Items;
  }

  if (data.LastEvaluatedKey) {
    result.LastEvaluatedKey = data.LastEvaluatedKey;

    delete data.LastEvaluatedKey;
  }

  return callback(null, Merge({}, data, result));
};

Table.prototype.runQuery = function (params, callback) {
  const self = this;

  if (!callback) {
    return Promises.wrap(this, this.runQuery, [params]);
  }
  self.sendRequest('query', params, internals.deserializeItems(self, callback));
};

Table.prototype.runScan = function (params, callback) {
  const self = this;

  if (!callback) {
    return Promises.wrap(this, this.runscan, [params]);
  }
  self.sendRequest('scan', params, internals.deserializeItems(self, callback));
};

Table.prototype.runBatchGetItems = function (params, callback) {
  const self = this;
  if (!callback) {
    return Promises.wrap(this, this.runBatchGetItems, [params]);
  }
  self.sendRequest('batchGet', params, callback);
};

internals.attributeDefinition = (schema, key) => {
  let type = schema._modelDatatypes[key];

  if (type === 'DATE') {
    type = 'S';
  }

  return {
    AttributeName: key,
    AttributeType: type
  };
};

internals.keySchema = (hashKey, rangeKey) => {
  const result = [{
    AttributeName: hashKey,
    KeyType: 'HASH'
  }];

  if (rangeKey) {
    result.push({
      AttributeName: rangeKey,
      KeyType: 'RANGE'
    });
  }

  return result;
};

internals.secondaryIndex = (schema, params) => {
  const projection = params.projection || { ProjectionType: 'ALL' };

  return {
    IndexName: params.name,
    KeySchema: internals.keySchema(schema.hashKey, params.rangeKey),
    Projection: projection
  };
};

internals.globalIndex = (indexName, params) => {
  const projection = params.projection || { ProjectionType: 'ALL' };

  return {
    IndexName: indexName,
    KeySchema: internals.keySchema(params.hashKey, params.rangeKey),
    Projection: projection,
    ProvisionedThroughput: {
      ReadCapacityUnits: params.readCapacity || 1,
      WriteCapacityUnits: params.writeCapacity || 1
    }
  };
};

Table.prototype.createTable = function (options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  if (!callback) {
    return Promises.wrap(this, this.createTable, [options]);
  }
  const attributeDefinitions = [];

  attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.hashKey));

  if (self.schema.rangeKey) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.rangeKey));
  }

  const localSecondaryIndexes = [];

  Foreach(self.schema.secondaryIndexes, params => {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    localSecondaryIndexes.push(internals.secondaryIndex(self.schema, params));
  });

  const globalSecondaryIndexes = [];

  Foreach(self.schema.globalIndexes, (params, indexName) => {
    if (!Find(attributeDefinitions, { AttributeName: params.hashKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.hashKey));
    }

    if (params.rangeKey && !Find(attributeDefinitions, { AttributeName: params.rangeKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    }

    globalSecondaryIndexes.push(internals.globalIndex(indexName, params));
  });

  const keySchema = internals.keySchema(self.schema.hashKey, self.schema.rangeKey);

  const params = {
    AttributeDefinitions: attributeDefinitions,
    TableName: self.tableName(),
    KeySchema: keySchema,
    ProvisionedThroughput: {
      ReadCapacityUnits: options.readCapacity || 1,
      WriteCapacityUnits: options.writeCapacity || 1
    }
  };

  if (localSecondaryIndexes.length >= 1) {
    params.LocalSecondaryIndexes = localSecondaryIndexes;
  }

  if (globalSecondaryIndexes.length >= 1) {
    params.GlobalSecondaryIndexes = globalSecondaryIndexes;
  }

  if (options.hasOwnProperty('streamSpecification') && typeof options.streamSpecification === 'object') {
    params.StreamSpecification = {
      StreamEnabled: options.streamSpecification.streamEnabled || false
    };
    if (params.StreamSpecification.StreamEnabled) {
      params.StreamSpecification.StreamViewType = options.streamSpecification.streamViewType || 'NEW_AND_OLD_IMAGES';
    }
  }

  self.sendRequest('createTable', params, callback);
};

Table.prototype.describeTable = function (callback) {

  const params = {
    TableName: this.tableName()
  };

  this.sendRequest('describeTable', params, callback);
};

Table.prototype.deleteTable = function (callback) {
  if (!callback) {
    return Promises.wrap(this, this.deleteTable);
  }

  const params = {
    TableName: this.tableName()
  };

  this.sendRequest('deleteTable', params, callback);
};

Table.prototype.updateTable = function (throughput, callback) {

  const self = this;
  if (typeof throughput === 'function' && !callback) {
    callback = throughput;
    throughput = {};
  }

  callback = callback || (() => {});
  throughput = throughput || {};

  Async.parallel([
    Async.apply(internals.syncIndexes, self),
    Async.apply(internals.updateTableCapacity, self, throughput),
  ], callback);
};

internals.updateTableCapacity = (table, throughput, callback) => {
  const params = {};

  if (Has(throughput, 'readCapacity') || Has(throughput, 'writeCapacity')) {
    params.ProvisionedThroughput = {};

    if (Has(throughput, 'readCapacity')) {
      params.ProvisionedThroughput.ReadCapacityUnits = throughput.readCapacity;
    }

    if (Has(throughput, 'writeCapacity')) {
      params.ProvisionedThroughput.WriteCapacityUnits = throughput.writeCapacity;
    }
  }

  if (!IsEmpty(params)) {
    params.TableName = table.tableName();
    table.sendRequest('updateTable', params, callback);
  } else {
    return callback();
  }
};

internals.syncIndexes = (table, callback) => {
  callback = callback || (() => {});

  table.describeTable((err, data) => {
    if (err) {
      return callback(err);
    }

    const missing = Values(internals.findMissingGlobalIndexes(table, data));
    if (IsEmpty(missing)) {
      return callback();
    }

    // UpdateTable only allows one new index per UpdateTable call
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.html#GSI.OnlineOps.Creating
    const maxIndexCreationsAtaTime = 5;
    Async.mapLimit(missing, maxIndexCreationsAtaTime, (params, callback) => {
      const attributeDefinitions = [];

      if (!_.find(attributeDefinitions, { AttributeName: params.hashKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.hashKey));
      }

      if (params.rangeKey && !_.find(attributeDefinitions, { AttributeName: params.rangeKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.rangeKey));
      }

      const currentWriteThroughput = data.Table.ProvisionedThroughput.WriteCapacityUnits;
      const newIndexWriteThroughput = _.ceil(currentWriteThroughput * 1.5);
      params.writeCapacity = params.writeCapacity || newIndexWriteThroughput;

      table.log.info('adding index %s to table %s', params.name, table.tableName());

      const updateParams = {
        TableName: table.tableName(),
        AttributeDefinitions: attributeDefinitions,
        GlobalSecondaryIndexUpdates: [{ Create: internals.globalIndex(params.name, params) }]
      };

      table.sendRequest('updateTable', updateParams, callback);
    }, callback);
  });
};

internals.findMissingGlobalIndexes = (table, data) => {

  if (data === null || data === undefined) {
    // table does not exist
    return table.schema.globalIndexes;
  }
  const indexData = Get(data, 'Table.GlobalSecondaryIndexes');
  const existingIndexNames = Map(indexData, 'IndexName');

  const missing = Reduce(table.schema.globalIndexes, (result, idx, indexName) => {
    if (!Includes(existingIndexNames, idx.name)) {
      result[indexName] = idx;
    }

    return result;
  }, {});
  return missing;
};
