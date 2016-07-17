{ isString, isArray, isUndefined } = require 'lodash'
{ ObjectId, Binary } = require 'mongodb'

exports.rewriteId = (model = {}, schema) ->
  if model._id
    model.id = model._id

  delete model._id

  model

exports.rewriteIds = (models, schema) ->
  if isArray models
    models.map (model) ->
      exports.rewriteId model, schema

exports.normalizeId = (value) ->
  if exports.matchMongoId value.id
    value._id = ObjectId value.id
  else
    value._id = value.id or ObjectId.createPk()

  delete value.id

  value

exports.normalizeIds = (values) ->
  values.map exports.normalizeId

exports.parseUpdateData = (data) ->
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
  if id is null or isUndefined id
    return false

  if typeof id.toString != 'undefined'
    id = id.toString()

  id.match /^[a-fA-F0-9]{24}$/
