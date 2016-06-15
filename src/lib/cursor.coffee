{ Readable } = require 'readable-stream'

class Cursor extends Readable
  constructor: (@cursor) ->
    super
      objectMode: true
      highWaterMark: 0

  next: (callback) ->
    if @cursor.cursorState.dead or @cursor.cursorState.killed
      return callback null, null
    else
      @cursor.next callback
    this

  rewind: (callback) ->
    @cursor.rewind callback
    this

  toArray: (callback) ->
    array = []

    iterate = =>
      @next (err, obj) ->
        if err or not obj
          return callback err, array
        array.push obj
        iterate()

    iterate()

  map: (mapfn, callback) ->
    array = []

    iterate = =>
      @next (err, obj) ->
        if err or not obj
          return callback err, array
        array.push mapfn obj
        iterate()

    iterate()

  forEach: (fn) ->

    iterate = ->
      @next (err, obj) ->
        return fn err if err
        fn err, obj
        return if not obj
        iterate()

    iterate()

  count: (callback) ->
    @cursor.count false, @opts, callback

  size: (callback) ->
    @cursor.count true, @opts, callback

  explain: (callback) ->
    @cursor.explain callback

  destroy: (callback = ->) ->
    if not @cursor.close
      return callback()

    @cursor.close callback

  _read: ->
    @next (err, data) =>
      if err
        return @emit 'error', err
      @push data

[ 'batchSize'
  'hint'
  'limit'
  'maxTimeMS'
  'max'
  'min'
  'skip'
  'snapshot'
  'sort'
].forEach (opt) ->
  Cursor.prototype[opt] = (obj, callback) ->
    @_opts[opt] = obj

    if callback
      return @toArray callback

    this

module.exports = Cursor
