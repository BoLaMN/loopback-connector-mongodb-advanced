{ ObjectId } = require 'mongodb'
{ eachSeries } = require 'async'

class Bulk
  constructor: (@collection, @ordered) ->
    @updates = []
    @deletes = []
    @inserts = []

    @total = 0

  find: (query) ->
    upsert = false

    self = @

    remove: (limit = 0) ->
      self.deletes.push
        q: query
        limit: limit
      self.total++
      return

    update: (obj, multi = true) ->
      self.updates.push
        q: query
        u: obj
        multi: multi
        upsert: upsert
      self.total++
      return

    upsert: ->
      upsert = true
      this

    removeOne: ->
      @remove 1

    updateOne: (obj) ->
      @update obj, false

  insert: (document) ->
    if not document._id
      document._id = ObjectID.createPk()

    @documents.push document
    @total++

  tojson: ->
    nInsertOps: @deletes.length
    nUpdateOps: @updates.length
    nRemoveOps: @documents.length
    nBatches: @total

  toString: ->
    JSON.stringify @tojson()

  build: ->
    cmds = []
    name = @collection.s.name

    if @updates.length
      cmds.push
        update: name
        updates: @updates
        ordered: @ordered
        writeConcern: w: 1

    if @deletes.length
      cmds.push
        delete: name
        deletes: @deletes
        ordered: @ordered
        writeConcern: w: 1

    if @documents.length
      cmds.push
        insert: name
        documents: @documents
        ordered: @ordered
        writeConcern: w: 1

    cmds

  _execute: (cmd, i, done) ->
    @collection.s.db.command cmd, (err, { n }) ->
      if err
        return done err
      counts[cmd[0]] += n
      done()

  execute: (cb = ->) ->
    counts =
      insert: 0
      delete: 0
      update: 0

    eachSeries @build(), @_execute, (err) ->
      if err
        return cb err

      cb null,
        writeErrors: []
        writeConcernErrors: []
        nInserted: counts.insert
        nUpserted: counts.update
        nMatched: 0
        nModified: 0
        nRemoved: counts.delete
        upserted: []
        ok: 1

module.exports = Bulk
