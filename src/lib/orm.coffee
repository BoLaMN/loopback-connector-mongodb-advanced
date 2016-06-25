debug = require('debug')('loopback:connector:mongodb-advanced')

Query = require './query'

{ Connector } = require 'loopback-connector'
{ normalizeId, rewriteId, parseUpdateData } = require './utils'

class ORM extends Connector
  ###*
  # Find matching model instances by the filter
  #
  # @param {String} model The model name
  # @param {Object} filter The filter
  # @param {Function} [callback] The callback function
  ###
  all: (modelName, filter, options = {}, callback) ->
    debug 'all', modelName, filter

    collection = @collection modelName
    model = @model modelName

    { filter, options } = new Query filter, model.model
    { where, aggregate, fields } = filter

    if aggregate
      aggregate = [
        { '$match': where }
        { '$group': filter.aggregateGroup }
      ]

      collection.aggregate aggregate, options, rewriteId
        .tap (results) ->
          debug 'all.aggregate.callback', modelName, results
        .asCallback callback
    else
      collection.find where, fields, options, rewriteId
        .tap (results) ->
          debug 'all.find.callback', modelName, results
        .asCallback callback

  ###*
  # Count the number of instances for the given model
  #
  # @param {String} model The model name
  # @param {Function} [callback] The callback function
  # @param {Object} filter The filter for where
  #
  ###
  count: (modelName, filter, options = {}, callback) ->
    debug 'count', modelName, filter

    if typeof filter == 'object'
      delete filter.fields

    collection = @collection modelName
    model = @model modelName

    { filter, options } = new Query filter, model.model
    { where } = filter

    collection.count where
      .tap (results) ->
        debug 'count.callback', modelName, results
      .asCallback callback

  ###*
  # Create a new model instance for the given data
  # @param {String} model The model name
  # @param {Object} data The model data
  # @param {Function} [callback] The callback function
  ###
  create: (modelName, data, options = {}, callback) ->
    debug 'create', modelName, data

    collection = @collection modelName

    collection.insert data, safe: true
      .tap (results) ->
        debug 'create.callback', modelName, results
      .asCallback callback

  ###*
  # Delete a model instance by id
  # @param {String} model The model name
  # @param {*} id The id value
  # @param [callback] The callback function
  ###
  destroy: (modelName, id, options = {}, callback) ->
    debug 'delete', modelName, id

    collection = @collection modelName

    collection.remove _id: id, true
      .tap (results) ->
        debug 'delete.callback', modelName, results
      .asCallback callback

  ###*
  # Delete all instances for the given model
  # @param {String} model The model name
  # @param {Object} [where] The filter for where
  # @param {Function} [callback] The callback function
  ###
  destroyAll: (modelName, filter, options = {}, callback) ->
    debug 'destroyAll', modelName, filter

    if typeof filter is 'object'
      delete filter.fields

    collection = @collection modelName
    model = @model modelName

    { filter, options } = new Query filter, model.model
    { where, fields } = filter

    finish = (err, results) ->
      if err
        return callback err

      debug 'destroyAll.callback', results

      if not Array.isArray results
        results = [ results ]

      results = results.map (result) ->
        id: result

      callback null, result

    collection.remove where, options, finish
      .tap (results) ->
        debug 'destroyAll.callback', modelName, results
      .asCallback finish

  ###*
  # Check if a model instance exists by id
  # @param {String} model The model name
  # @param {*} id The id value
  # @param {Function} [callback] The callback function
  #
  ###
  exists: (modelName, id, options = {}, callback) ->
    debug 'exists', modelName, id

    collection = @collection modelName

    collection.findOne _id: id, options
      .tap (results) ->
        debug 'findOne.callback', modelName, results
      .asCallback callback

  ###*
  # Find a model instance by id
  # @param {String} model The model name
  # @param {*} id The id value
  # @param {Function} [callback] The callback function
  ###
  find: (modelName, id, options = {}, callback) ->
    debug 'find', modelName, id

    collection = @collection modelName

    collection.findOne _id: id , options, rewriteId
      .tap (results) ->
        debug 'find.callback', modelName, results
      .asCallback callback

  ###*
  # Find a matching model instances by the filter
  # or create a new instance
  #
  # Only supported on mongodb 2.6+
  #
  # @param {String} model The model name
  # @param {Object} data The model instance data
  # @param {Object} filter The filter
  # @param {Function} [callback] The callback function
  ###
  findOrCreate: (modelName, filter = {}, data, callback) ->
    debug 'findOrCreate', modelName, filter, data

    collection = @collection modelName

    callback null, {}

  ###*
  # Replace properties for the model instance data
  # @param {String} model The name of the model
  # @param {*} id The instance id
  # @param {Object} data The model data
  # @param {Object} options The options object
  # @param {Function} [callback] The callback function
  ###
  replaceById: (modelName, id, data, options = {}, callback) ->
    debug 'replaceById', modelName, id, data

    @replaceWithOptions model, id, data, upsert: false
      .tap (results) ->
        debug 'replaceById.callback', modelName, results
      .asCallback callback

  ###*
  # Replace model instance if it exists or create a new one if it doesn't
  #
  # @param {String} model The name of the model
  # @param {Object} data The model instance data
  # @param {Object} options The options object
  # @param {Function} [callback] The callback function
  ###
  replaceOrCreate: (modelName, data, options = {}, callback) ->
    debug 'replaceOrCreate', modelName, data

    @replaceWithOptions model, id, data, upsert: true
      .tap (results) ->
        debug 'replaceOrCreate.callback', modelName, results
      .asCallback callback

  ###*
  # Update a model instance with id
  # @param {String} model The name of the model
  # @param {Object} id The id of the model instance
  # @param {Object} data The property/value pairs to be
  #                 updated or inserted if {upsert: true}
  #                 is passed as options
  # @param {Object} options The options you want to pass
  #                 for update, e.g, {upsert: true}
  # @callback {Function} [callback] Callback function
  ###
  replaceWithOptions: (modelName, id, data, options = {}, callback) ->
    debug 'updateWithOptions', modelName, id, data

    collection = @collection modelName

    collection.update { _id: normalizeId(id) }, data, options
      .tap (results) ->
        debug 'updateWithOptions.callback', modelName, results
      .asCallback callback

  ###*
  # Save the model instance for the given data
  # @param {String} model The model name
  # @param {Object} data The model data
  # @param {Function} [callback] The callback function
  ###
  save: (modelName, data, options = {}, callback) ->
    debug 'save', modelName, data

    collection = @collection modelName

    collection.save data, options
      .tap (results) ->
        debug 'save.callback', modelName, results
      .asCallback callback

  ###*
  # Update all matching instances
  # @param {String} model The model name
  # @param {Object} where The search criteria
  # @param {Object} data The property/value pairs to be updated
  # @callback {Function} callback Callback function
  ###
  update: (modelName, filter, data, options = {}, callback) ->
    debug 'update', modelName, filter, data

    if typeof filter == 'object'
      delete filter.fields

    collection = @collection modelName
    model = @model modelName

    { filter, options } = new Query filter, model.model
    { where } = filter

    collection.update where, data, options
      .tap (results) ->
        debug 'update.callback', modelName, results
      .asCallback callback

  updateAll: @update

  ###*
  # Update properties for the model instance data
  # @param {String} model The model name
  # @param {Object} data The model data
  # @param {Function} [callback] The callback function
  ###
  updateAttributes: (modelName, id, data, options = {}, callback) ->
    debug 'updateAttributes', modelName, id, data

    data = parseUpdateData data
    collection = @collection modelName

    id = data[ @idName(modelName) ]
    sort = [ '_id', 'asc' ]

    collection.findAndModify { _id: id }, [ sort ], data, {}
      .tap (results) ->
        debug 'updateAttributes.callback', modelName, results
      .asCallback callback

  ###*
  # Update if the model instance exists with the same
  # id or create a new instance
  #
  # @param {String} model The model name
  # @param {Object} data The model instance data
  # @param {Function} [callback] The callback function
  ###
  updateOrCreate: @save

module.exports = ORM