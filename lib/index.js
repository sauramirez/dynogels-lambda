'use strict';

const Util = require('util');
const AWS = require('aws-sdk');
const DocClient = AWS.DynamoDB.DocumentClient;
const Table = require('./table');
const Promises = require('./promises');
const Schema = require('./schema');
const Serializer = require('./serializer');
const Batch = require('./batch');
const Item = require('./item');
const Foreach = require('lodash.foreach');
const IsFunction = require('lodash.isfunction');
const CreateTables = require('./createTables');

const dynogels = module.exports;

dynogels.AWS = AWS;

const internals = {};

dynogels.log = dynogels.log || {
  info: () => null,
  warn: () => null
};

dynogels.dynamoDriver = internals.dynamoDriver = (driver) => {

  if (driver) {
    internals.dynamodb = driver;

    const docClient = internals.loadDocClient(driver);
    internals.updateDynamoDBDocClientForAllModels(docClient);
  }
  else {
    internals.dynamodb = internals.dynamodb || new dynogels.AWS.DynamoDB({ apiVersion: '2012-08-10' });
  }

  return internals.dynamodb;
};

dynogels.documentClient = internals.documentClient = (docClient) => {

  if (docClient) {
    internals.docClient = docClient;
    internals.dynamodb = docClient.service;
    internals.updateDynamoDBDocClientForAllModels(docClient);
  }
  else {
    internals.loadDocClient();
  }

  return internals.docClient;
};

internals.updateDynamoDBDocClientForAllModels = (docClient) => {

  Foreach(dynogels.models, (model) => {

    return model.config({ docClient });
  });
};

internals.loadDocClient = (driver) => {

  if (driver) {
    internals.docClient = new DocClient({ service: driver });
  }
  else {
    internals.docClient = internals.docClient || new DocClient({ service: internals.dynamoDriver() });
  }

  return internals.docClient;
};

internals.compileModel = (name, schema, log) => {
  const tableName = name.toLowerCase();

  const table = new Table(tableName, schema, Serializer, internals.loadDocClient(), log);

  const Model = function (attrs) {

    Item.call(this, attrs, table);
  };

  Util.inherits(Model, Item);

  Model.get = table.get.bind(table);
  Model.create = table.create.bind(table);
  Model.update = table.update.bind(table);
  Model.destroy = table.destroy.bind(table);
  Model.query = table.query.bind(table);
  Model.scan = table.scan.bind(table);
  Model.parallelScan = table.parallelScan.bind(table);

  Model.getItems = Batch(table, Serializer).getItems;
  Model.batchGetItems = Batch(table, Serializer).getItems;

  // table ddl methods
  Model.createTable = table.createTable.bind(table);
  Model.updateTable = table.updateTable.bind(table);
  Model.describeTable = table.describeTable.bind(table);
  Model.deleteTable = table.deleteTable.bind(table);
  Model.tableName = table.tableName.bind(table);

  table.itemFactory = Model;

  Model.log = log;

  // hooks
  Model.after = table.after.bind(table);
  Model.before = table.before.bind(table);

  Model.__defineGetter__('docClient', () => table.docClient);
  Model.__defineGetter__('schema', () => table.schema);

  Model.config = (config) => {

    config = config || {};
    if (config.tableName) {
      table.config.name = config.tableName;
    }

    if (config.docClient) {
      table.docClient = config.docClient;
    }
    else if (config.dynamodb) {
      table.docClient = new DocClient({ service: config.dynamodb });
    }

    return table.config;
  };

  return dynogels.model(name, Model);
};

internals.addModel = (name, model) => {

  dynogels.models[name] = model;
  return dynogels.models[name];
};

dynogels.reset = () => {

  dynogels.models = {};
};

dynogels.Set = function () {

  return internals.docClient.createSet.apply(internals.docClient, arguments);
};

dynogels.define = (modelName, config) => {

  if (config.constructor === Function) {
    throw new Error('define no longer accepts schema callback, migrate to new api');
  }

  const schema = new Schema(config);

  const compiledTable = internals.compileModel(modelName, schema, config.log || dynogels.log);

  return compiledTable;
};

dynogels.model = (name, model) => {

  if (model) {
    internals.addModel(name, model);
  }

  return dynogels.models[name] || null;
};

dynogels.createTables = (options, callback) => {

  if (IsFunction(options)) {
    callback = options;
    options = {};
  }

  options = options || {};
  if (!callback) {
    return Promises.wrap(this, this.createTables, [options]);
  }

  return CreateTables(dynogels.models, options, callback);
};

dynogels.types = Schema.types;

dynogels.reset();
