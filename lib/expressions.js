'use strict';

const Utils = require('./utils');
const Serializer = require('./serializer');
const Includes = require('lodash.includes');
const IsDate = require('lodash.isdate');
const IsPlainObject = require('lodash.isplainobject');
const IsString = require('lodash.isstring');
const IsEmpty = require('lodash.isempty');
const Reduce = require('lodash.reduce');
const Keys = require('lodash.keys');

const internals = {};

internals.actionWords = ['SET', 'ADD', 'REMOVE', 'DELETE'];

internals.regexMap = Reduce(internals.actionWords, (result, key) => {
  result[key] = new RegExp(`${key}\\s*(.+?)\\s*(SET|ADD|REMOVE|DELETE|$)`);
  return result;
}, {});

// explanation http://stackoverflow.com/questions/3428618/regex-to-find-commas-that-arent-inside-and
internals.splitOperandsRegex = new RegExp(/\s*(?![^(]*\)),\s*/);

internals.match = (actionWord, str) => {
  const match = internals.regexMap[actionWord].exec(str);

  if (match && match.length >= 2) {
    return match[1].split(internals.splitOperandsRegex);
  } else {
    return null;
  }
};

exports.parse = str => Reduce(internals.actionWords, (result, actionWord) => {
  result[actionWord] = internals.match(actionWord, str);
  return result;
}, {});

exports.serializeUpdateExpression = (schema, item) => {
  const datatypes = schema._modelDatatypes;

  const data = Utils.omitPrimaryKeys(schema, item);

  const memo = {
    expressions: {},
    attributeNames: {},
    values: {},
  };

  memo.expressions = Reduce(internals.actionWords, (result, key) => {
    result[key] = [];

    return result;
  }, {});

  const result = Reduce(data, (result, value, key) => {
    const valueKey = `:${key}`;
    const nameKey = `#${key}`;

    if (value === null || (IsString(value) && IsEmpty(value))) {
      result.expressions.REMOVE.push(nameKey);
      result.attributeNames[nameKey] = key;
    } else if (IsPlainObject(value) && value.$add) {
      result.expressions.ADD.push(`${nameKey} ${valueKey}`);
      result.values[valueKey] = Serializer.serializeAttribute(value.$add, datatypes[key]);
      result.attributeNames[nameKey] = key;
    } else if (IsPlainObject(value) && value.$del) {
      result.expressions.DELETE.push(`${nameKey} ${valueKey}`);
      result.values[valueKey] = Serializer.serializeAttribute(value.$del, datatypes[key]);
      result.attributeNames[nameKey] = key;
    } else {
      result.expressions.SET.push(`${nameKey} = ${valueKey}`);
      result.values[valueKey] = Serializer.serializeAttribute(value, datatypes[key]);
      result.attributeNames[nameKey] = key;
    }

    return result;
  }, memo);

  return result;
};

exports.stringify = expressions => Reduce(expressions, (result, value, key) => {
  if (!IsEmpty(value)) {
    if (Array.isArray(value)) {
      result.push(`${key} ${value.join(', ')}`);
    } else {
      result.push(`${key} ${value}`);
    }
  }

  return result;
}, []).join(' ');

internals.formatAttributeValue = val => {
  if (IsDate(val)) {
    return val.toISOString();
  }

  return val;
};

internals.isFunctionOperator = operator => Includes(['attribute_exists',
  'attribute_not_exists',
  'attribute_type',
  'begins_with',
  'contains',
  'NOT contains',
  'size'], operator);

internals.uniqAttributeValueName = (key, existingValueNames) => {
  const cleanedKey = key.replace(/\./g, '_').replace(/\W/g, '');
  let potentialName = `:${cleanedKey}`;
  let idx = 1;

  while (Includes(existingValueNames, potentialName)) {
    idx++;
    potentialName = `:${cleanedKey}_${idx}`;
  }

  return potentialName;
};

exports.buildFilterExpression = (key, operator, existingValueNames, val1, val2) => {
  // IN filter expression is unlike all the others where val1 is an array of values
  if (operator === 'IN') {
    return internals.buildInFilterExpression(key, existingValueNames, val1);
  }

  let v1 = internals.formatAttributeValue(val1);
  const v2 = internals.formatAttributeValue(val2);

  if (operator === 'attribute_exists' && v1 === false) {
    operator = 'attribute_not_exists';
    v1 = null;
  } else if (operator === 'attribute_exists' && v1 === true) {
    v1 = null;
  }

  const keys = key.split('.');
  const path = `#${keys.join('.#').replace(/[^\w\.#]/g, '')}`;
  const v1ValueName = internals.uniqAttributeValueName(key, existingValueNames);
  const v2ValueName = internals.uniqAttributeValueName(key, [v1ValueName].concat(existingValueNames));

  let statement = '';

  if (internals.isFunctionOperator(operator)) {
    if (v1 !== null && v1 !== undefined) {
      statement = `${operator}(${path}, ${v1ValueName})`;
    } else {
      statement = `${operator}(${path})`;
    }
  } else if (operator === 'BETWEEN') {
    statement = `${path} BETWEEN ${v1ValueName} AND ${v2ValueName}`;
  } else {
    statement = [path, operator, v1ValueName].join(' ');
  }

  const attributeValues = {};

  if (v1 !== null && v1 !== undefined) {
    attributeValues[v1ValueName] = v1;
  }

  if (v2 !== null && v2 !== undefined) {
    attributeValues[v2ValueName] = v2;
  }

  const attributeNames = {};
  keys.forEach((key) => {
    attributeNames[`#${key.replace(/[^\w\.]/g, '')}`] = key;
  });

  return {
    attributeNames: attributeNames,
    statement: statement,
    attributeValues: attributeValues
  };
};

internals.buildInFilterExpression = (key, existingValueNames, values) => {
  const path = `#${key}`;

  const attributeNames = {};
  attributeNames[path.split('.')[0]] = key.split('.')[0];

  const attributeValues = Reduce(values, (result, val) => {
    const existing = Keys(result).concat(existingValueNames);
    const p = internals.uniqAttributeValueName(key, existing);
    result[p] = internals.formatAttributeValue(val);
    return result;
  }, {});

  return {
    attributeNames: attributeNames,
    statement: `${path} IN (${Keys(attributeValues)})`,
    attributeValues: attributeValues
  };
};
