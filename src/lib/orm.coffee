debug = require('debug')('loopback:connector:mongodb-advanced')

Query = require './query'

{ Connector } = require 'loopback-connector'
{ rewriteIds } = require './utils'

class ORM extends Connector
  ###*
  # Find matching model instances by the filter
  #
  # @param {String} model The model name
  # @param {Object} filter The filter
  # @param {Function} [callback] The callback function
  ###
  all: (model, filter, options = {}, callback) ->
    debug 'all', model, filter

    collection = @collection model
    properties = @properties model

    try
      query = new Query filter, properties
    catch err
      return callback err

    where = query.filter.where or {}

    finish = (err, results) ->
      callback err, rewriteIds results, properties

    if query.aggregate
      aggregate = [
        { '$match': where }
        { '$group': query.aggregateGroup }
      ]

      collection.aggregate aggregate, finish
    else
      delete query.filter.where

      collection.find where, query.fields, query, finish

  ###*
  # Count the number of instances for the given model
  #
  # @param {String} model The model name
  # @param {Function} [callback] The callback function
  # @param {Object} filter The filter for where
  #
  ###
  count: (model, filter, options = {}, callback) ->
    debug 'count', model, filter

    if typeof filter == 'object'
      delete filter.fields

    collection = @collection model
    properties = @properties model

    try
      query = new Query filter, properties
    catch err
      return callback err

    where = query.filter.where or {}

    collection.count where, callback

  ###*
  # Create a new model instance for the given data
  # @param {String} model The model name
  # @param {Object} data The model data
  # @param {Function} [callback] The callback function
  ###
  create: (model, data, options = {}, callback) ->
    debug 'create', model, data

    collection = @collection model

    collection.insert data, safe: true, callback

  ###*
  # Delete a model instance by id
  # @param {String} model The model name
  # @param {*} id The id value
  # @param [callback] The callback function
  ###
  destroy: (model, id, options = {}, callback) ->
    debug 'delete', model, id

    collection = @collection model

    collection.remove _id: id, true, callback

  ###*
  # Delete all instances for the given model
  # @param {String} model The model name
  # @param {Object} [where] The filter for where
  # @param {Function} [callback] The callback function
  ###
  destroyAll: (model, filter, options = {}, callback) ->
    debug 'destroyAll', model, filter

    if typeof filter is 'object'
      delete filter.fields

    collection = @collection model
    properties = @properties model

    try
      query = new Query filter, properties
    catch err
      return callback err

    where = query.filter.where or {}

    collection.remove where, options, (err, results) ->
      if err
        return callback err

      resultsArray = []

      if !Array.isArray(results)
        resultsArray.push id: results
        return callback(null, resultsArray)

      results.forEach (result) ->
        resultsArray.push id: result

      callback null, rewriteIds(resultArray, properties)

  ###*
  # Check if a model instance exists by id
  # @param {String} model The model name
  # @param {*} id The id value
  # @param {Function} [callback] The callback function
  #
  ###
  exists: (model, id, options = {}, callback) ->
    debug 'exists', model, id

    collection = @collection model

    collection.findOne _id: id, options, callback

  ###*
  # Find a model instance by id
  # @param {String} model The model name
  # @param {*} id The id value
  # @param {Function} [callback] The callback function
  ###
  find: (model, id, options = {}, callback) ->
    debug 'find', model, id

    collection = @collection model

    collection.findOne _id: id, options, callback

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
  findOrCreate: (model, filter = {}, data, callback) ->
    debug 'findOrCreate', model, filter, data

    collection = @collection model

    callback null, {}

  ###*
  # Replace properties for the model instance data
  # @param {String} model The name of the model
  # @param {*} id The instance id
  # @param {Object} data The model data
  # @param {Object} options The options object
  # @param {Function} [callback] The callback function
  ###
  replaceById: (model, id, data, options = {}, callback) ->
    debug 'replaceById', model, id, data

    collection = @collection model

    callback null, {}

  ###*
  # Replace model instance if it exists or create a new one if it doesn't
  #
  # @param {String} model The name of the model
  # @param {Object} data The model instance data
  # @param {Object} options The options object
  # @param {Function} [callback] The callback function
  ###
  replaceOrCreate: (model, data, options = {}, callback) ->
    debug 'replaceOrCreate', model, data

    collection = @collection model

    callback null, {}

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
  replaceWithOptions: (model, id, data, options = {}, callback) ->
    debug 'updateWithOptions', model, id, data

    collection = @collection model

    callback null, {}

  ###*
  # Save the model instance for the given data
  # @param {String} model The model name
  # @param {Object} data The model data
  # @param {Function} [callback] The callback function
  ###
  save: (model, data, options = {}, callback) ->
    debug 'save', model, data

    collection = @collection model

    collection.save data, options, callback

  ###*
  # Update all matching instances
  # @param {String} model The model name
  # @param {Object} where The search criteria
  # @param {Object} data The property/value pairs to be updated
  # @callback {Function} callback Callback function
  ###
  update: (model, filter, data, options = {}, callback) ->
    debug 'update', model, filter, data

    if typeof filter == 'object'
      delete filter.fields

    collection = @collection model
    properties = @properties model

    try
      query = new Query filter, properties
    catch err
      return callback err

    where = query.filter.where or {}

    collection.update where, data, options, callback

  updateAll: @update

  ###*
  # Update properties for the model instance data
  # @param {String} model The model name
  # @param {Object} data The model data
  # @param {Function} [callback] The callback function
  ###
  updateAttributes: (model, id, data, options = {}, callback) ->
    debug 'updateAttributes', model, id, data

    collection = @collection model

    callback null, {}

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