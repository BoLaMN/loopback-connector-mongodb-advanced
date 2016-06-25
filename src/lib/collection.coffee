debug = require('debug')('loopback:connector:mongodb-advanced')

Cursor = require './cursor'
Bulk = require './bulk'

{ ObjectID } = require 'mongodb'
{ clone, extend, isFunction, isBoolean } = require 'lodash'

writeOpts =
  writeConcern: w: 1
  ordered: true

noop = ->

class Collection
  constructor: (@collection) ->

  find: (query, projection, opts, mapFn, callback) ->
    if isFunction query
      return @find {}, null, null, null, query

    if isFunction projection
      return @find query, null, null, null, projection

    if isFunction opts
      return @find query, projection, null, null, opts

    console.log 'args length', arguments.length, mapFn
    if arguments.length is 4
      return @find query, projection, opts, null, callback

    cursor = new Cursor @collection.find(query, projection, opts)

    if callback
      if mapFn
        return cursor.map(mapFn).asCallback callback
      else
        return cursor.toArray().asCallback callback

    cursor

  findOne: (query, projection, mapFn, callback) ->
    if isFunction query
      return @findOne {}, null, query

    if isFunction projection
      return @findOne query, null, projection

    if isFunction mapFn and not isFunction callback
      return @findOne query, projection, null, callback

    @find query, projection, mapFn
      .next()
      .asCallback callback

  findAndModify: (opts, callback) ->

    done = (err, result) ->
      if err
        return callback err

      callback null, result.value, result.lastErrorObject or n: 0

    @execute 'findAndModify', opts
      .asCallback done

  count: (query, callback) ->
    if isFunction query
      return @count {}, query

    @find query
      .count()
      .asCallback callback

  distinct: (field, query, callback) ->
    params =
      key: field
      query: query

    done = (err, result) ->
      if err
        return callback err

      callback null, result.values

    @execute 'distinct', params
      .asCallback done

  insert: (docOrDocs, opts, callback) ->
    if not opts and not callback
      return @insert docOrDocs, {}, noop

    if isFunction opts
      return @insert docOrDocs, {}, opts

    if opts and not callback
      return @insert docOrDocs, opts, noop

    docs = if Array.isArray(docOrDocs) then docOrDocs else [ docOrDocs ]
    instances = clone docs

    i = 0

    while i < docs.length
      if not docs[i]._id
        id = ObjectID.createPk()
        docs[i]._id = id
      instances[i].id = docs[i]._id.toString()
      delete instances[i]._id
      i++

    @collection.insert docs, extend(writeOpts, opts)
      .then (results) -> instances
      .asCallback callback

  update: (query, update, opts, callback) ->
    if not opts and not callback
      return @update query, update, {}, noop

    if isFunction opts
      return @update query, update, {}, opts

    done = (err, result) ->
      if err
        return callback err

      callback null, result.result

    @collection.update query, update, extend(writeOpts, opts)
      .asCallback done

  save: (doc, opts = {}, callback) ->
    if isFunction opts
      return @save doc, {}, opts

    if doc._id
      @update { _id: doc._id }, doc, extend({ upsert: true }, opts)
        .asCallback callback
    else
      @insert doc, opts
        .asCallback callback

  remove: (query, opts, callback) ->
    if isFunction query
      return @remove {}, { justOne: false }, query

    if isFunction opts
      return @remove query, { justOne: false }, opts

    if isBoolean opts
      return @remove query, { justOne: opts }, callback

    if not opts
      return @remove query, { justOne: false }, callback

    if not callback
      return @remove query, opts, noop

    deleteOperation = if opts.justOne then 'deleteOne' else 'deleteMany'

    @collection[deleteOperation] query, extend(opts, writeOpts)
      .then (result) ->
        result.result
      .asCallback callback

  rename: (name, opts, callback) ->
    if isFunction opts
      return @rename name, {}, opts

    if not opts
      return @rename name, {}, noop

    if not callback
      return @rename name, noop

    @collection.rename name, opts
      .asCallback callback

  drop: (callback) ->
    @execute 'drop'
      .asCallback callback

  stats: (callback) ->
    @execute 'collStats'
      .asCallback callback

  mapReduce: (map, reduce, opts, callback) ->
    if isFunction opts
      return @mapReduce map, reduce, {}, opts

    if not callback
      return @mapReduce map, reduce, opts, noop

    @collection.mapReduce map, reduce, opts
      .asCallback callback

  execute: (cmd, opts, callback) ->
    if isFunction opts
      return @execute cmd, null, opts

    opts = opts or {}

    obj = {}
    obj[cmd] = @collection.s.name

    Object.keys(opts).forEach (key) ->
      obj[key] = opts[key]
      return

    @collection.s.db.command obj
      .asCallback callback

  toString: ->
    @collection.s.name

  dropIndexes: (callback) ->
    @execute 'dropIndexes', index: '*'
      .asCallback callback

  dropIndex: (index, callback) ->
    @execute 'dropIndexes', index: index
      .asCallback callback

  createIndex: (index, opts, callback) ->
    if isFunction opts
      return @createIndex index, {}, opts

    if not opts
      return @createIndex index, {}, noop

    if not callback
      return @createIndex index, opts, noop

    @collection.createIndex index, opts
      .asCallback callback

  ensureIndex: (index, opts, callback) ->
    if isFunction opts
      return @ensureIndex index, {}, opts

    if not opts
      return @ensureIndex index, {}, noop

    if not callback
      return @ensureIndex index, opts, noop

    @collection.ensureIndex index, opts
      .asCallback callback

  getIndexes: (callback) ->
    @collection.indexes()
      .asCallback callback

  reIndex: (callback) ->
    @execute 'reIndex'
      .asCallback callback

  isCapped: (callback) ->
    @collection.isCapped()
      .asCallback callback

  group: (doc, callback) ->
    key = doc.key or doc.keyf

    @collection.group key, doc.cond, doc.initial, doc.reduce, doc.finalize
      .asCallback callback

  aggregate: (pipeline, opts, mapFn, callback) ->
    if isFunction mapFn and not isFunction callback
      return @aggregate pipeline, opts, null, callback

    cursor = new Cursor @collection.aggregate pipeline, opts

    if callback
      if mapFn
        return cursor.map(mapFn).asCallback callback
      else
        return cursor.toArray().asCallback callback

    cursor

  bulk: (ordered = true) ->
    new Bulk @collection, ordered

module.exports = Collection
