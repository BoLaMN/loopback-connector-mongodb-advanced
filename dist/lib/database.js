var Collection, MongoClient, MongoDB, ORM, debug, isFunction, isString, ref,
  extend1 = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty,
  slice = [].slice;

debug = require('debug')('loopback:connector:mongodb-advanced');

MongoClient = require('mongodb').MongoClient;

ref = require('lodash'), isFunction = ref.isFunction, isString = ref.isString;

ORM = require('./orm');

Collection = require('./collection');

MongoDB = (function(superClass) {
  extend1(MongoDB, superClass);

  function MongoDB(url, dataSource) {
    this.url = url;
    this.dataSource = dataSource;
    MongoDB.__super__.constructor.call(this);
    this.name = 'mongodb-advanced';
    this.settings = this.dataSource.settings || {};
    debug('Settings: %j', this.settings);
  }

  MongoDB.prototype.getTypes = function() {
    return ['db', 'nosql', 'mongodb'];
  };

  MongoDB.prototype.getDefaultIdType = function() {
    return this.dataSource.ObjectID;
  };

  MongoDB.prototype.collection = function(model) {
    return new Collection(this.db.collection(this.collectionName(model)));
  };

  MongoDB.prototype.collectionName = function(model) {
    var modelClass, ref1;
    modelClass = this._models[model];
    return ((ref1 = modelClass.settings[this.name]) != null ? ref1.collection : void 0) || model;
  };

  MongoDB.prototype.connect = function(callback) {
    return MongoClient.connect(this.url, this.settings, (function(_this) {
      return function(err, db) {
        _this.db = db;
        return callback(err, db);
      };
    })(this));
  };

  MongoDB.prototype.disconnect = function(callback) {
    debug('disconnect');
    this.db.close();
    if (callback) {
      return process.nextTick(callback);
    }
  };

  MongoDB.prototype.ping = function(callback) {
    if (callback == null) {
      callback = function() {};
    }
    return this.db.collection('dummy').findOne({
      _id: 1
    }, callback);
  };

  MongoDB.prototype.execute = function(opts, callback) {
    var tmp;
    if (callback == null) {
      callback = function() {};
    }
    if (isString(opts)) {
      tmp = opts;
      opts = {};
      opts[tmp] = 1;
    }
    return this.db.command(opts, callback);
  };

  MongoDB.prototype.listCollections = function(callback) {
    return this.db.listCollections().toArray(callback);
  };

  MongoDB.prototype.properties = function(model) {
    return this._models[model].properties;
  };

  MongoDB.prototype.getCollectionNames = function(callback) {
    return this.listCollections(function(err, collections) {
      if (err) {
        return callback(err);
      }
      return callback(null, collections.map(function(collection) {
        return collection.name;
      }));
    });
  };

  MongoDB.prototype.createCollection = function(name, opts, callback) {
    var cmd;
    if (isFunction(opts)) {
      return this.createCollection(name, {}, opts);
    }
    cmd = {
      create: name
    };
    Object.keys(opts).forEach(function(opt) {
      return cmd[opt] = opts[opt];
    });
    return this.execute(cmd, callback);
  };

  MongoDB.prototype.stats = function(scale, callback) {
    if (isFunction(scale)) {
      return this.stats(1, scale);
    }
    return this.execute({
      dbStats: 1,
      scale: scale
    }, callback);
  };

  MongoDB.prototype.dropDatabase = function(callback) {
    return this.execute('dropDatabase', callback);
  };

  MongoDB.prototype.createUser = function(usr, callback) {
    var cmd;
    cmd = extend({
      createUser: usr.user
    }, usr);
    delete cmd.user;
    return this.execute(cmd, callback);
  };

  MongoDB.prototype.dropUser = function(username, callback) {
    return this.execute({
      dropUser: username
    }, callback);
  };

  MongoDB.prototype["eval"] = function() {
    var args, callback, cmd, fn, i;
    fn = arguments[0], args = 3 <= arguments.length ? slice.call(arguments, 1, i = arguments.length - 1) : (i = 1, []), callback = arguments[i++];
    cmd = {
      "eval": fn.toString(),
      args: args
    };
    return this.execute(cmd, function(err, res) {
      if (err) {
        return callback(err);
      }
      return callback(null, res.retval);
    });
  };

  MongoDB.prototype.getLastErrorObj = function(callback) {
    return this.execute('getLastError', callback);
  };

  MongoDB.prototype.getLastError = function(callback) {
    return this.execute('getLastError', function(err, res) {
      if (err) {
        return callback(err);
      }
      return callback(null, res.err);
    });
  };

  MongoDB.prototype.toString = function() {
    return this.db.s.databaseName;
  };

  return MongoDB;

})(ORM);

module.exports = MongoDB;
