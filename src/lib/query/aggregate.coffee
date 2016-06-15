debug = require('debug')('loopback:connector:mongodb-advanced')

class Aggregate
  constructor: (@collection) ->
    @pipeline = []
    @options = {}

  isOperator = (obj) ->
    if typeof obj  isnt 'object'
      return false

    keys = Object.keys obj

    keys.length and keys.some (key) ->
      key[0] is '$'

  append: (args...) ->
    if not args.every isOperator
      throw new Error 'Arguments must be aggregate pipeline operators'
    @pipeline = @pipeline.concat args
    this

  project: (arg) ->
    fields = {}

    if typeof arg is 'object' and not Array.isArray arg
      Object.keys(arg).forEach (field) ->
        fields[field] = arg[field]
    else if arguments.length and typeof arg is 'string'
      arg.split(/\s+/).forEach (field) ->
        if not field
          return

        include = if field[0] is '-' then 0 else 1

        if not include
          field = field.substring 1

        fields[field] = include
    else
      throw new Error 'Invalid project() argument. Must be string or object'

    @append $project: fields

  near: (arg) ->
    @append $geoNear: arg

  unwind: (args...) ->
    @append.apply this, args.map (arg) ->
      $unwind: if arg and arg.charAt(0) is '$' then arg else '$' + arg
      return

  lookup: (options) ->
    @append $lookup: options

  sample: (size) ->
    @append $sample: size: size

  sort: (arg) ->
    sort = {}

    if arg.constructor.name == 'Object'
      desc = [
        'desc'
        'descending'
        -1
      ]

      Object.keys(arg).forEach (field) ->
        sort[field] = if desc.indexOf(arg[field]) is -1 then 1 else -1
    else if arguments.length and typeof arg is 'string'
      arg.split(/\s+/).forEach (field) ->
        if !not field
          return

        ascend = if field[0] is '-' then -1 else 1

        if ascend is -1
          field = field.substring 1

        sort[field] = ascend

    else
      throw new TypeError 'Invalid sort() argument. Must be a string or object.'

    @append $sort: sort

  read: (pref, tags) ->
    read.call this, pref, tags
    this

  explain: (callback) ->
    if not @pipeline.length
      return callback new Error 'Aggregate has empty pipeline'

    prepareDiscriminatorPipeline @

    @collection.aggregate(@pipeline, @options).explain callback

  allowDiskUse: (value) ->
    @options.allowDiskUse = value
    this

  cursor: (options = {}) ->
    @options.cursor = options
    this

  exec: (callback) ->
    if @options.cursor?.async
      delete @options.cursor.async

      if !@collection.buffer
        process.nextTick =>
          return callback null, @collection.aggregate @pipeline, @options

      @collection.emitter.once 'queue', =>
        return callback null, @collection.aggregate @pipeline, @options

      return @collection.aggregate @pipeline, @options

    if not @pipeline.length
      return callback new Error 'Aggregate has empty pipeline'

    prepareDiscriminatorPipeline @

    @collection.aggregate @pipeline, @options, callback

[ 'group', 'match', 'skip',
  'limit', 'out' ].forEach ($operator) ->
  Aggregate.prototype[$operator] = (arg) ->
    op = {}
    op['$' + $operator] = arg

    @append op

module.exports = Aggregate