var Cursor, Promise, Readable,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

Readable = require('readable-stream').Readable;

Promise = require('bluebird');

Cursor = (function(superClass) {
  extend(Cursor, superClass);

  function Cursor(cursor) {
    this.cursor = cursor;
    Cursor.__super__.constructor.call(this, {
      objectMode: true,
      highWaterMark: 0
    });
  }

  Cursor.prototype.next = function(callback) {
    if (this.cursor.cursorState.dead || this.cursor.cursorState.killed) {
      return callback(null, null);
    } else {
      this.cursor.next().asCallback(callback);
    }
    return this;
  };

  Cursor.prototype.rewind = function(callback) {
    this.cursor.rewind(callback);
    return this;
  };

  Cursor.prototype.toArray = function(callback) {
    return new Promise(function(resolve, reject) {
      var array, iterate;
      array = [];
      iterate = (function(_this) {
        return function() {
          return _this.next(function(err, obj) {
            if (err) {
              return reject(err);
            }
            if (!obj) {
              return resolve(array);
            }
            array.push(obj);
            return iterate();
          });
        };
      })(this);
      return iterate();
    });
  };

  Cursor.prototype.map = function(mapfn, callback) {
    var array, iterate;
    array = [];
    iterate = (function(_this) {
      return function() {
        return _this.next(function(err, obj) {
          if (err || !obj) {
            return callback(err, array);
          }
          array.push(mapfn(obj));
          return iterate();
        });
      };
    })(this);
    return iterate();
  };

  Cursor.prototype.forEach = function(fn) {
    var iterate;
    iterate = function() {
      return this.next(function(err, obj) {
        if (err) {
          return fn(err);
        }
        fn(err, obj);
        if (!obj) {
          return;
        }
        return iterate();
      });
    };
    return iterate();
  };

  Cursor.prototype.count = function(callback) {
    return this.cursor.count(false, this.opts, callback);
  };

  Cursor.prototype.size = function(callback) {
    return this.cursor.count(true, this.opts, callback);
  };

  Cursor.prototype.explain = function(callback) {
    return this.cursor.explain(callback);
  };

  Cursor.prototype.destroy = function(callback) {
    if (callback == null) {
      callback = function() {};
    }
    if (!this.cursor.close) {
      return callback();
    }
    return this.cursor.close(callback);
  };

  Cursor.prototype._read = function() {
    return this.next((function(_this) {
      return function(err, data) {
        if (err) {
          return _this.emit('error', err);
        }
        return _this.push(data);
      };
    })(this));
  };

  return Cursor;

})(Readable);

['batchSize', 'hint', 'limit', 'maxTimeMS', 'max', 'min', 'skip', 'snapshot', 'sort'].forEach(function(opt) {
  return Cursor.prototype[opt] = function(obj, callback) {
    this._opts[opt] = obj;
    if (callback) {
      return this.toArray(callback);
    }
    return this;
  };
});

module.exports = Cursor;
