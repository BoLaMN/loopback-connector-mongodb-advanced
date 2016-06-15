debug = require('debug')('loopback:connector:mongodb-advanced')

{ MongoClient } = require 'mongodb'
{ isFunction, isString } = require 'lodash'

ORM = require './orm'
Collection = require './collection'

class MongoDB extends ORM
  constructor: (@url, @dataSource) ->
    super()

    @name = 'mongodb-advanced'
    @settings = @dataSource.settings or {}

    debug 'Settings: %j', @settings

  getTypes: ->
    [ 'db', 'nosql', 'mongodb' ]

  getDefaultIdType: ->
    @dataSource.ObjectID

  collection: (model) ->
    new Collection @db.collection @collectionName(model)

  collectionName: (model) ->
    modelClass = @_models[model]
    modelClass.settings[@name]?.collection or model

  connect: (callback) ->
    MongoClient.connect @url, @settings, (err, db) =>
      @db = db

      callback err, db

  disconnect: (callback) ->
    debug 'disconnect'

    @db.close()

    if callback
      process.nextTick callback

  ping: (callback = ->) ->
    @db.collection('dummy').findOne { _id: 1 }, callback

  execute: (opts, callback = ->) ->
    if isString opts
      tmp = opts
      opts = {}
      opts[tmp] = 1

    @db.command opts, callback

  listCollections: (callback) ->
    @db.listCollections().toArray callback

  properties: (model) ->
    @_models[model].properties

  getCollectionNames: (callback) ->
    @listCollections (err, collections) ->
      if err
        return callback(err)

      callback null, collections.map (collection) ->
        collection.name

  createCollection: (name, opts, callback) ->
    if isFunction opts
      return @createCollection name, {}, opts

    cmd = create: name

    Object.keys(opts).forEach (opt) ->
      cmd[opt] = opts[opt]

    @execute cmd, callback

  stats: (scale, callback) ->
    if isFunction scale
      return @stats 1, scale

    @execute dbStats: 1, scale: scale, callback

  dropDatabase: (callback) ->
    @execute 'dropDatabase', callback

  createUser: (usr, callback) ->
    cmd = extend { createUser: usr.user }, usr
    delete cmd.user

    @execute cmd, callback

  dropUser: (username, callback) ->
    @execute { dropUser: username }, callback

  eval: (fn, args..., callback) ->
    cmd =
      eval: fn.toString()
      args: args

    @execute cmd, (err, res) ->
      if err
        return callback err

      callback null, res.retval

  getLastErrorObj: (callback) ->
    @execute 'getLastError', callback

  getLastError: (callback) ->
    @execute 'getLastError', (err, res) ->
      if err
        return callback err

      callback null, res.err

  toString: ->
    @db.s.databaseName

module.exports = MongoDB