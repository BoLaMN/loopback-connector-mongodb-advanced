'use strict'

should = require('should')
init = require('./init')

{ Db } = require 'mongodb'

describe 'MongoDB Advanced connector', ->

  it 'can connect', (done) ->
    init.getDataSource null, (err, res) ->
      if err
        return done(err)

      res.should.be.Object()
      res.should.have.property 'connected', true
      res.should.have.property('connector').which.is.Object()

      db = res
      connector = res.connector

      done()

  it 'can connect', (done) ->
    connector.connect (err, res) ->
      if err
        return done(err)

      res.should.be.instanceof Db
      done()

  it 'can disconnect', (done) ->
    db.disconnect done

  it 'can disconnect', (done) ->
    connector.disconnect (err, res) ->
      if err
        return done(err)

      res.should.equal true
      done()

  it 'can connect twice the same time', (done) ->
    connector.connect()
    connector.connect done

  it 'can disconnect twice the same time', (done) ->
    connector.disconnect()
    connector.disconnect done

  it 'can connect and disconnect', (done) ->
    connector.connect()
    connector.disconnect done

  it 'can connect with a host', (done) ->
    init.getDataSource
      host: 'localhost'
    .then (res) ->
      res.should.be.Object()
      res.should.have.property 'connected', true
      res.should.have.property('connector').which.is.Object()
      res.disconnect done
    .catch done

  it 'can connect with a port', (done) ->
    init.getDataSource port: '27017'
    .then (res) ->
      res.should.be.Object()
      res.should.have.property 'connected', true
      res.should.have.property('connector').which.is.Object()
      res.disconnect done
    .catch done

  it 'can connect with a host and a port', (done) ->
    init.getDataSource
      host: 'localhost'
      port: '27017'
    .then (res) ->
      res.should.be.Object()
      res.should.have.property 'connected', true
      res.should.have.property('connector').which.is.Object()
      res.disconnect done
    .catch done

  it 'cannot connect with a wrong port', (done) ->
    init.getDataSource
      port: '1234'
    .then (res) ->
      done new Error('expected an error')
    .catch (err) ->
      err.should.be.instanceof Error
      done()

  it 'can connect with a URL', (done) ->
    init.getDataSource
      url: 'mongodb://localhost:27017'
    .then (res) ->
      res.should.be.Object()
      res.should.have.property 'connected', true
      res.should.have.property('connector').which.is.Object()
      res.disconnect done
    .catch done
