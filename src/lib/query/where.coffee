'use strict'

debug = require('debug')('loopback:connector:mongodb-advanced')

{ isObject, isString, isArray
  isUndefined, isRegExp, isFunction } = require 'lodash'

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

  parse: (where) ->

    if where is null or typeof where isnt 'object'
      return @query

    Object.keys(where).forEach (propName) =>
      cond = where[propName]

      if propName is 'id'
        propName = '_id'

      if propName in [ 'and', 'or', 'nor' ]
        if Array.isArray cond
          cond = cond.map (c) =>
            @parse c

        @[propName] propName, cond
        delete @query[propName]

      spec = false
      options = null

      if cond and cond.constructor.name is 'Object'
        options = cond.options
        spec = Object.keys(cond)[0]
        cond = cond[spec]

      if spec
        return @[spec] propName, cond

      else
        @query[propName] = cond or $type: 10

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
  # Between
  #
  # @param {String} key - key
  # @param {Mixed} value - value
  # @api public
  ###

  between: (key, [ gte, lte ]) ->
    @lastKey = key
    @gte gte

    @lastKey = key
    @lte lte

    this

  ###*
  # Same as .where(), only less flexible
  #
  # @param {String} key - key
  # @param {Mixed} value - value
  # @api public
  ###

  inq: (key, value) ->
    @in key, value

  neq: (key, value) ->
    @ne key, value

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
      @query[key] ?= {}
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