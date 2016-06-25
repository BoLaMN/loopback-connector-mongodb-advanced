var MongoDB, ObjectID, defaults, url;

MongoDB = require('./lib/database');

ObjectID = require('mongodb').ObjectID;

defaults = require('lodash').defaults;

url = function(arg) {
  var path, settings;
  settings = arg.settings;
  defaults(settings, {
    hostname: '127.0.0.1',
    port: 27017,
    database: 'test'
  });
  path = 'mongodb://';
  if (settings.username && settings.password) {
    path += [settings.username, ':', settings.password, '@'].join('');
  }
  path += [settings.hostname, ':', settings.port].join('');
  return path + '/' + settings.database;
};

exports.initialize = function(dataSource, callback) {
  var connector;
  defaults(dataSource.settings, {
    safe: false,
    w: 1
  });
  dataSource.ObjectID = function(id) {
    var e;
    if (id instanceof ObjectID || typeof id !== 'string') {
      return id;
    }
    if (/^[0-9a-fA-F]{24}$/.test(id)) {
      try {
        return new ObjectID(id);
      } catch (_error) {
        e = _error;
      }
    }
    return id;
  };
  connector = new MongoDB(url(dataSource), dataSource);
  dataSource.connector = connector;
  dataSource.connector.connect(callback);
};
