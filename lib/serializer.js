'use strict';

const Utils = require('./utils');
const AWS = require('aws-sdk');
const Foreach = require('lodash.foreach');
const Has = require('lodash.has');
const IsBoolean = require('lodash.isboolean');
const IsDate = require('lodash.isdate');
const IsPlainObject = require('lodash.isplainobject');
const IsObject = require('lodash.isobject');
const IsString = require('lodash.isstring');
const Map = require('lodash.map');
const MapValues = require('lodash.mapvalues');
const Reduce = require('lodash.reduce');

const serializer = module.exports;

const internals = {};

internals.docClient = new AWS.DynamoDB.DocumentClient();

internals.createSet = (value) => {

  if (Array.isArray(value)) {
    return internals.docClient.createSet(value);
  }
  return internals.docClient.createSet([value]);
};

const serialize = internals.serialize = {

  binary: function (value) {

    if (IsString(value)) {
      return Utils.strToBin(value);
    }

    return value;
  },

  date: function (value) {
    if (IsDate(value)) {
      return value.toISOString();
    } else {
      return new Date(value).toISOString();
    }
  },

  boolean: function (value) {
    if (value && value !== 'false') {
      return true;
    } else {
      return false;
    }
  },

  stringSet: function (value) {
    return internals.createSet(value, 'S');
  },

  numberSet: function (value) {
    return internals.createSet(value, 'N');
  },

  binarySet: function (value) {
    let bins = value;
    if (!Array.isArray(value)) {
      bins = [value];
    }

    const vals = Map(bins, serialize.binary);
    return internals.createSet(vals, 'B');
  }
};

internals.deserializeAttribute = value => {
  if (IsObject(value) && value.detectType.constructor === Function && Array.isArray(value.values)) {
    // value is a Set object from document client
    return value.values;
  }
  return value;
};

internals.serializeAttribute = serializer.serializeAttribute = (value, type, options) => {
  if (!type) { // if type is unknown, possibly because its an dynamic key return given value
    return value;
  }

  if (value === null) {
    return null;
  }

  options = options || {};

  switch (type) {
    case 'DATE':
      return serialize.date(value);
    case 'BOOL':
      return serialize.boolean(value);
    case 'B':
      return serialize.binary(value);
    case 'NS':
      return serialize.numberSet(value);
    case 'SS':
      return serialize.stringSet(value);
    case 'BS':
      return serialize.binarySet(value);
    default:
      return value;
  }
};

serializer.buildKey = (hashKey, rangeKey, schema) => {
  const obj = {};

  if (IsPlainObject(hashKey)) {
    obj[schema.hashKey] = hashKey[schema.hashKey];

    if (schema.rangeKey && hashKey[schema.rangeKey] !== null && hashKey[schema.rangeKey] !== undefined) {
      obj[schema.rangeKey] = hashKey[schema.rangeKey];
    }
    Foreach(schema.globalIndexes, keys => {
      if (Has(hashKey, keys.hashKey)) {
        obj[keys.hashKey] = hashKey[keys.hashKey];
      }

      if (Has(hashKey, keys.rangeKey)) {
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });

    Foreach(schema.secondaryIndexes, keys => {
      if (Has(hashKey, keys.rangeKey)) {
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });
  } else {
    obj[schema.hashKey] = hashKey;

    if (schema.rangeKey && rangeKey !== null && rangeKey !== undefined) {
      obj[schema.rangeKey] = rangeKey;
    }
  }

  return serializer.serializeItem(schema, obj);
};

serializer.serializeItem = (schema, item, options) => {
  options = options || {};

  const serialize = (item, datatypes) => {
    datatypes = datatypes || {};

    if (!item) {
      return null;
    }

    return Reduce(item, (result, val, key) => {
      if (options.expected && val !== undefined && val.constructor === Object && IsBoolean(val.Exists)) {
        result[key] = val;
        return result;
      }

      if (IsPlainObject(val)) {
        result[key] = serialize(val, datatypes[key]);
        return result;
      }

      const attr = internals.serializeAttribute(val, datatypes[key], options);

      if (attr !== null || options.returnNulls) {
        if (options.expected) {
          result[key] = { Value: attr };
        } else {
          result[key] = attr;
        }
      }

      return result;
    }, {});
  };

  return serialize(item, schema._modelDatatypes);
};

serializer.serializeItemForUpdate = (schema, action, item) => {
  const datatypes = schema._modelDatatypes;

  const data = Utils.omitPrimaryKeys(schema, item);
  return Reduce(data, (result, value, key) => {
    if (value === null) {
      result[key] = { Action: 'DELETE' };
    } else if (IsPlainObject(value) && value.$add) {
      result[key] = { Action: 'ADD', Value: internals.serializeAttribute(value.$add, datatypes[key]) };
    } else if (IsPlainObject(value) && value.$del) {
      result[key] = { Action: 'DELETE', Value: internals.serializeAttribute(value.$del, datatypes[key]) };
    } else {
      result[key] = { Action: action, Value: internals.serializeAttribute(value, datatypes[key]) };
    }

    return result;
  }, {});
};

serializer.deserializeItem = item => {
  if (item === null) {
    return null;
  }

  const formatter = data => {
    let map = MapValues;

    if (Array.isArray(data)) {
      map = Map;
    }

    return map(data, value => {
      let result;

      if (IsPlainObject(value)) {
        result = formatter(value);
      } else if (Array.isArray(value)) {
        result = formatter(value);
      } else {
        result = internals.deserializeAttribute(value);
      }

      return result;
    });
  };

  return formatter(item);
};
