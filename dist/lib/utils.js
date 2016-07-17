var Binary, ObjectId, isArray, isString, isUndefined, ref, ref1;

ref = require('lodash'), isString = ref.isString, isArray = ref.isArray, isUndefined = ref.isUndefined;

ref1 = require('mongodb'), ObjectId = ref1.ObjectId, Binary = ref1.Binary;

exports.rewriteId = function(model, schema) {
  if (model == null) {
    model = {};
  }
  if (model._id) {
    model.id = model._id;
  }
  delete model._id;
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
  if (exports.matchMongoId(value.id)) {
    value._id = ObjectId(value.id);
  } else {
    value._id = value.id || ObjectId.createPk();
  }
  delete value.id;
  return value;
};

exports.normalizeIds = function(values) {
  return values.map(exports.normalizeId);
};

exports.parseUpdateData = function(data) {
  var acceptedOperators, i, parsedData, usedOperators;
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
  if (id === null || isUndefined(id)) {
    return false;
  }
  if (typeof id.toString !== 'undefined') {
    id = id.toString();
  }
  return id.match(/^[a-fA-F0-9]{24}$/);
};
