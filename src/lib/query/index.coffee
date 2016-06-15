Aggregate = require './aggregate'

{ isString, isPlainObject } = require 'lodash'
{ ObjectId } = require 'mongodb'

class Query
  constructor: (filter, @properties) ->
    @fields = @parseFields filter.fields

    @checkAggregate filter

    delete filter.fields

    @filter = @normalizeFilter(filter) or {}

    this

  checkAggregate: (filter) ->
    aggregateOptions = [
      'lookup'
      'groupBy'
      'sum'
      'average'
      'min'
      'max'
    ]

    aggregates = Object.keys(filter).filter (filterKey) ->
      filterKey in aggregateOptions

    if not aggregates.length
      return filter

    @aggregate = new Aggregate filter

    return

  normalizeFilter: (filter) ->
    filter.where = @parseWhere filter.where
    filter.sort = @parseSort filter.sort
    filter

  parseWhere: (where) ->
    query = {}

    if where is null or typeof where isnt 'object'
      return query

    Object.keys(where).forEach (propName) ->
      cond = where[propName]

      if propName in [ 'and', 'or', 'nor' ]
        if Array.isArray cond
          cond = cond.map (c) =>
            @parseWhere c

        query['$' + propName] = cond
        delete query[propName]

      if propName is 'id'
        propName = '_id'

      spec = false
      options = null

      if isPlainObject cond
        options = cond.options
        spec = Object.keys(cond)[0]
        cond = cond[spec]

      if spec
        query[propName] = switch spec
          when 'between'
            $gte: cond[0]
            $lte: cond[1]
          when 'inq'
            $in: cond
          when 'nin'
            $nin: cond
          when 'like'
            $regex: new RegExp cond, options
          when 'nlike'
            $not: new RegExp cond, options
          when 'neq'
            $ne: cond
          when 'regexp'
            $regex: cond

        if not query[propName]
          query[propName] = {}
          query[propName]['$' + spec] = cond
      else
        query[propName] = cond or $type: 10

    query

  parseSort: (order) ->
    sort = {}

    if order
      if isString order
        order = order.split ','

      index = 0
      len = order.length

      while index < len
        [input, key, m ] = /\s+([\w\d]+) (A|DE)SC$/.exec order[index]

        if key == 'id'
          key = '_id'

        if m and m[1] == 'DE'
          sort[key] = -1
        else
          sort[key] = 1
        index++
    else
      sort = _id: 1

    sort

  parseFields: (original = []) ->
    fields = {}
    original.forEach (field) ->
      fields[field] = 1
    fields

module.exports = Query