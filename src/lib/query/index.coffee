'use strict'

debug = require('debug')('loopback:connector:mongodb-advanced')

Aggregate = require './aggregate'
Where = require './where'

{ isObject, isString, isFunction
  isArray, isPlainObject, extend } = require 'lodash'
{ ObjectId } = require 'mongodb'

###*
* Query
###

class Query
  constructor: (filter, @model) ->
    @filter =
      include: null
      aggregate: []
      fields: {}
      options: {}
      where: {}

    for own key, value of filter
      if isFunction @[key]
        @[key] value
      else
        debug 'query filter ' + key + ' not found, value: ', value
  ###*
  # set where query
  #
  # @param {String} key
  # @api public
  ###

  where: (conditions) ->
    { query } = new Where conditions
    @filter.where = query

    this

  ###*
  # set aggregate query
  #
  # @param {String} key
  # @api public
  ###

  aggregate: (conditions) ->
    { query } = new Aggregate conditions
    @filter.aggregate = query

    this

  ###*
  # Handle iterating over include/exclude methods
  #
  # @param {String} key
  # @param {Mixed} value
  # @api public
  ###

  fields: (fields, value = 1) ->
    if isArray fields
      fields.forEach (key) =>
        @fields key

    if isObject fields
      keys = Object.keys fields
      keys.forEach (key) =>
        @fields key, fields[key]

    if isString fields
      @filter.fields[fields] = value

    this

  ###*
  # Exclude fields from result
  #
  # @param {String} key
  # @api public
  ###

  exclude: (fields) ->
    @fields fields, 0

    this

  ###*
  # Set query limit
  #
  # @param {Number} limit - limit number
  # @api public
  ###

  limit: (limit) ->
    @filter.options.limit = limit

    this

  include: (includes) ->
    @filter.include = includes

    this

  ###*
  # Set query skip
  #
  # @param {Number} skip - skip number
  # @api public
  ###

  skip: (skip) ->
    @filter.options.skip = skip

    this

  ###*
  # Alias for skip
  #
  # @param {String} offset
  # @api public
  ###

  offset: (offset) ->
    @skip offset

  ###*
  # Search using text index
  #
  # @param {String} text
  # @api public
  ###

  search: (text) ->
    @where '$text': '$search': text

    this

  ###*
  # Sort query results
  #
  # @param {Object} sort - sort params
  # @api public
  ###

  sort: (sorts, value) ->
    if isArray sorts
      sorts.forEach (sort) =>
        @sort.apply this, sort.split ' '

    if isString value
      matches = sorts.match /([\w\d]+) (A|DE)SC/gi

      if matches
        return @sort matches

      if sorts is 'id'
        sorts = '_id'

      @filter.options.sort ?= {}
      @filter.options.sort[sorts] = if value is 'DE' then -1 else 1
    else
      @filter.options.sort = _id: 1

    this

module.exports = Query