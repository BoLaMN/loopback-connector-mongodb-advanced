Cursor = require './cursor'
Bulk = require './bulk'

{ ObjectID } = require 'mongodb'
{ extend, isFunction, isBoolean } = require 'lodash'

writeOpts =
  writeConcern: w: 1
  ordered: true

noop = ->

class Collection
  constructor: (@collection) ->

  find: (query, projection, opts, callback) ->
    if isFunction query
      return @find {}, null, null, query

    if isFunction projection
      return @find query, null, null, projection

    if isFunction opts
      return @find query, projection, null, opts

    cursor = new Cursor @collection.find(query, projection, opts)

    if callback
      return cursor.toArray callback

    cursor

  findOne: (query, projection, callback) ->
    if isFunction query
      return @findOne {}, null, query

    if isFunction projection
      return @findOne query, null, projection

    @find(query, projection).next callback

  findAndModify: (opts, callback) ->
    @execute 'findAndModify', opts, (err, result) ->
      if err
        return callback err

      callback null, result.value, result.lastErrorObject or n: 0

  count: (query, callback) ->
    if isFunction query
      return @count {}, query

    @find(query).count callback

  distinct: (field, query, callback) ->
    params =
      key: field
      query: query

    @execute 'distinct', params, (err, result) ->
      if err
        return callback err

      callback null, result.values

  insert: (docOrDocs, opts, callback) ->
    if not opts and not callback
      return @insert docOrDocs, {}, noop

    if isFunction opts
      return @insert docOrDocs, {}, opts

    if opts and not callback
      return @insert docOrDocs, opts, noop

    docs = if Array.isArray(docOrDocs) then docOrDocs else [ docOrDocs ]
    i = 0

    while i < docs.length
      if not docs[i]._id
        docs[i]._id = ObjectID.createPk()
      i++

    @collection.insert docs, extend(writeOpts, opts), (err) ->
      if err
        return callback err

      callback null, docOrDocs

  update: (query, update, opts, callback) ->
    if not opts and not callback
      return @update query, update, {}, noop

    if isFunction opts
      return @update query, update, {}, opts

    @collection.update query, update, extend(writeOpts, opts), (err, result) ->
      if err
        return callback err

      callback null, result.result

  save: (doc, opts, callback) ->
    if not opts and not callback
      return @save doc, {}, noop

    if isFunction opts
      return @save doc, {}, opts

    if not callback
      return @save doc, opts, noop

    if doc._id
      @update { _id: doc._id }, doc, extend({ upsert: true }, opts), callback
    else
      @insert doc, opts, callback

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

    finish = (err, result) ->
      if err
        return callback err

      callback null, result.result

    @collection[deleteOperation] query, extend(opts, writeOpts), finish

  rename: (name, opts, callback) ->
    if isFunction opts
      return @rename name, {}, opts

    if not opts
      return @rename name, {}, noop

    if not callback
      return @rename name, noop

    @collection.rename name, opts, callback

  drop: (callback) ->
    @execute 'drop', callback

  stats: (callback) ->
    @execute 'collStats', callback

  mapReduce: (map, reduce, opts, callback) ->
    if isFunction opts
      return @mapReduce map, reduce, {}, opts

    if not callback
      return @mapReduce map, reduce, opts, noop

    @collection.mapReduce map, reduce, opts, callback

  execute: (cmd, opts, callback) ->
    if isFunction opts
      return @execute cmd, null, opts

    opts = opts or {}

    obj = {}
    obj[cmd] = @collection.s.name

    Object.keys(opts).forEach (key) ->
      obj[key] = opts[key]
      return

    @collection.s.db.command obj, callback

  toString: ->
    @collection.s.name

  dropIndexes: (callback) ->
    @execute 'dropIndexes', { index: '*' }, callback

  dropIndex: (index, callback) ->
    @execute 'dropIndexes', { index: index }, callback

  createIndex: (index, opts, callback) ->
    if isFunction opts
      return @createIndex index, {}, opts

    if not opts
      return @createIndex index, {}, noop

    if not callback
      return @createIndex index, opts, noop

    @collection.createIndex index, opts, callback

  ensureIndex: (index, opts, callback) ->
    if isFunction opts
      return @ensureIndex index, {}, opts

    if not opts
      return @ensureIndex index, {}, noop

    if not callback
      return @ensureIndex index, opts, noop

    @collection.ensureIndex index, opts, callback

  getIndexes: (callback) ->
    @collection.indexes callback

  reIndex: (callback) ->
    @execute 'reIndex', callback

  isCapped: (callback) ->
    @collection.isCapped callback

  group: (doc, callback) ->
    @collection.group doc.key or doc.keyf, doc.cond, doc.initial, doc.reduce, doc.finalize, callback

  aggregate: (pipeline, opts, callback) ->
    strm = new Cursor @collection.aggregate pipeline, opts

    if callback
      return strm.toArray callback

    strm

  bulk: (ordered = true) ->
    new Bulk @collection, ordered

module.exports = Collection
