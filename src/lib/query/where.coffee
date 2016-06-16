'use strict'

debug = require('debug')('loopback:connector:mongodb-advanced')

{ isObject, isString, isArray
  isUndefined, isRegExp } = require 'lodash'

###*
* Where
###

class Where
  constructor: (conditions) ->
    @query = {}

    @parse conditions

    this

  ###*
  # Set "where" condition
  #
  # @param {String} key - key
  # @param {Mixed} value - value
  # @api public
  ###

  parse: (conditions, value) ->
    if isObject conditions
      keys = Object.keys conditions

      keys.forEach (key) =>
        @where key, conditions[key]

    if isString conditions
      if isUndefined value
        @lastKey = key
        return this

      if isRegExp value
        value = $regex: value

      if isArray value
        value = $in: value

      @query[key] = value

    this

  ###*
  # Match documents using $elemMatch
  #
  # @param {String} key
  # @param {Object} value
  # @api public
  ###

  matches: (key, value) ->
    if @lastKey
      value = key
      key = @lastKey

      @lastKey = null

    @query[key] = $elemMatch: value

    this

  match: ->
    @matches.apply this, arguments

  ###*
  # Same as .where(), only less flexible
  #
  # @param {String} key - key
  # @param {Mixed} value - value
  # @api public
  ###

  equals: (value) ->
    key = @lastKey

    @lastKey = null
    @query[key] = value

    this

  ###*
  # Set property that must or mustn't exist in resulting docs
  #
  # @param {String} key - key
  # @param {Boolean} exists - exists or not
  # @api public
  ###

  exists: (key, exists = true) ->
    if @lastKey
      exists = key
      key = @lastKey

      @lastKey = null

    @query[key] = $exists: exists

    this

[ 'lt', 'lte'
  'gt', 'gte'
  'in', 'nin'
  'ne'
].forEach (method) ->

  Where::[method] = (key, value) ->
    if @lastKey
      value = key
      key = @lastKey

      @lastKey = null

    operator = '$' + method
    hasValue = value isnt undefined

    if hasValue
      @query[key] = {}
      @query[key][operator] = value
    else
      @query[operator] = key

    this

  return

[ 'or', 'nor', 'and' ].forEach (method) ->

  Where::[method] = (args...) ->
    operator = '$' + method

    @query[operator] = args

    this

  return

module.exports = Where