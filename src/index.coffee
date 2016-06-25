MongoDB = require './lib/database'

{ ObjectID } = require 'mongodb'
{ defaults } = require 'lodash'

url = ({ settings }) ->
  defaults settings,
    hostname: '127.0.0.1'
    port: 27017
    database: 'test'

  path = 'mongodb://'

  if settings.username and settings.password
    path += [ settings.username, ':', settings.password, '@' ].join ''
  path += [ settings.hostname, ':', settings.port ].join ''

  path + '/' + settings.database

exports.initialize = (dataSource, callback) ->
  defaults dataSource.settings,
    safe: false
    w: 1

  dataSource.ObjectID = (id) ->
    if id instanceof ObjectID or typeof id isnt 'string'
      return id
    if /^[0-9a-fA-F]{24}$/.test id
      try
        return new ObjectID id
      catch e
    id

  connector = new MongoDB url(dataSource), dataSource

  dataSource.connector = connector
  dataSource.connector.connect callback

  return

