'use strict'

Promise = require 'bluebird'

{ DataSource } = require 'loopback-datasource-juggler'

exports.getDataSource = (customConfig, callback) ->
  new DataSource require('../'), customConfig or {}
    .asCallback callback

