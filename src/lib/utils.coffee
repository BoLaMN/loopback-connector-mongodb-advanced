{ isString, isArray } = require 'lodash'
{ ObjectId, Binary } = require 'mongodb'

exports.rewriteId = (model = {}, schema) ->
  if model._id
    if typeof model._id == 'object' and model._id._bsontype
      model.id = model._id.toString()
    else
      model.id = model._id
    delete model._id

  if !schema
    return model

  Object.keys(schema).forEach (key) ->
    foreignKey = schema[key].foreignKey or false

    if foreignKey and model[key] instanceof ObjectId
      model[key] = model[key].toString()

  model

exports.rewriteIds = (models, schema) ->
  if isArray models
    models.map (model) ->
      exports.rewriteId model, schema

exports.normalizeId = (value) ->
  if !value.id
    return
  if exports.matchMongoId value.id
    value._id = new ObjectId.createFromHexString value.id
  else
    value._id = cloneDeep value.id
  delete value.id

exports.normalizeIds = (values) ->
  values.map exports.normalizeId

exports.parseUpdateData = (modelClass, data, options = {}) ->
  parsedData = {}

  acceptedOperators = [
    '$currentDate'
    '$inc'
    '$max'
    '$min'
    '$mul'
    '$rename'
    '$setOnInsert'
    '$set'
    '$unset'
    '$addToSet'
    '$pop'
    '$pullAll'
    '$pull'
    '$pushAll'
    '$push'
    '$bit'
  ]

  usedOperators = 0
  i = 0

  while i < acceptedOperators.length
    if data[acceptedOperators[i]]
      parsedData[acceptedOperators[i]] = data[acceptedOperators[i]]
      usedOperators++
    i++

  if not usedOperators
    parsedData.$set = data

  parsedData

exports.normalizeResults = (models = [], schema) ->
  models.map (model) ->
    model = exports.rewriteIds(model, schema)
    Object.keys(model).forEach (key) ->
      if model[key] instanceof Binary and model[key]?.buffer
        model[key] = model[key].buffer
    model

exports.matchMongoId = (id) ->
  if id == null
    return false
  test = cloneDeep(id)
  if typeof test.toString != 'undefined'
    test = id.toString()
  if test.match(/^[a-fA-F0-9]{24}$/) then true else false
