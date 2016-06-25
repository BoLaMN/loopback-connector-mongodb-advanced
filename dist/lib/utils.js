var Binary, ObjectId, isArray, isString, ref, ref1;

ref = require('lodash'), isString = ref.isString, isArray = ref.isArray;

ref1 = require('mongodb'), ObjectId = ref1.ObjectId, Binary = ref1.Binary;

exports.rewriteId = function(model, schema) {
  if (model == null) {
    model = {};
  }
  if (model._id) {
    if (typeof model._id === 'object' && model._id._bsontype) {
      model.id = model._id.toString();
    } else {
      model.id = model._id;
    }
    delete model._id;
  }
  if (!schema) {
    return model;
  }
  Object.keys(schema).forEach(function(key) {
    var foreignKey;
    foreignKey = schema[key].foreignKey || false;
    if (foreignKey && model[key] instanceof ObjectId) {
      return model[key] = model[key].toString();
    }
  });
  return model;
};

exports.rewriteIds = function(models, schema) {
  if (isArray(models)) {
    return models.map(function(model) {
      return exports.rewriteId(model, schema);
    });
  }
};

exports.normalizeId = function(value) {
  if (!value.id) {
    return;
  }
  if (exports.matchMongoId(value.id)) {
    value._id = new ObjectId.createFromHexString(value.id);
  } else {
    value._id = cloneDeep(value.id);
  }
  return delete value.id;
};

exports.normalizeIds = function(values) {
  return values.map(exports.normalizeId);
};

exports.parseUpdateData = function(modelClass, data, options) {
  var acceptedOperators, i, parsedData, usedOperators;
  if (options == null) {
    options = {};
  }
  parsedData = {};
  acceptedOperators = ['$currentDate', '$inc', '$max', '$min', '$mul', '$rename', '$setOnInsert', '$set', '$unset', '$addToSet', '$pop', '$pullAll', '$pull', '$pushAll', '$push', '$bit'];
  usedOperators = 0;
  i = 0;
  while (i < acceptedOperators.length) {
    if (data[acceptedOperators[i]]) {
      parsedData[acceptedOperators[i]] = data[acceptedOperators[i]];
      usedOperators++;
    }
    i++;
  }
  if (!usedOperators) {
    parsedData.$set = data;
  }
  return parsedData;
};

exports.normalizeResults = function(models, schema) {
  if (models == null) {
    models = [];
  }
  return models.map(function(model) {
    model = exports.rewriteIds(model, schema);
    Object.keys(model).forEach(function(key) {
      var ref2;
      if (model[key] instanceof Binary && ((ref2 = model[key]) != null ? ref2.buffer : void 0)) {
        return model[key] = model[key].buffer;
      }
    });
    return model;
  });
};

exports.matchMongoId = function(id) {
  var test;
  if (id === null) {
    return false;
  }
  test = cloneDeep(id);
  if (typeof test.toString !== 'undefined') {
    test = id.toString();
  }
  if (test.match(/^[a-fA-F0-9]{24}$/)) {
    return true;
  } else {
    return false;
  }
};
