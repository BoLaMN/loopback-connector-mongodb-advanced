'use strict';
var Where, debug, isArray, isFunction, isObject, isRegExp, isString, isUndefined, matchMongoId, ref,
  slice = [].slice;

debug = require('debug')('loopback:connector:mongodb-advanced');

ref = require('lodash'), isObject = ref.isObject, isString = ref.isString, isArray = ref.isArray, isUndefined = ref.isUndefined, isRegExp = ref.isRegExp, isFunction = ref.isFunction;

matchMongoId = require('../utils').matchMongoId;


/**
* Where
 */

Where = (function() {
  function Where(conditions) {
    this.query = {};
    this.parse(conditions);
    this;
  }


  /**
   * Set "where" condition
  #
   * @param {String} key - key
   * @param {Mixed} value - value
   * @api public
   */

  Where.prototype.parse = function(where) {
    if (where === null || typeof where !== 'object') {
      return this.query;
    }
    Object.keys(where).forEach((function(_this) {
      return function(propName) {
        var cond, options, spec;
        cond = where[propName];
        if (propName === 'id') {
          propName = '_id';
        }
        if (propName === 'and' || propName === 'or' || propName === 'nor') {
          if (Array.isArray(cond)) {
            cond = cond.map(function(c) {
              return _this.parse(c);
            });
          }
          _this[propName](propName, cond);
          delete _this.query[propName];
        }
        spec = false;
        options = null;
        if (cond && cond.constructor.name === 'Object') {
          options = cond.options;
          spec = Object.keys(cond)[0];
          cond = cond[spec];
        }
        if (spec) {
          return _this[spec](propName, cond);
        } else {
          return _this.query[propName] = cond || {
            $type: 10
          };
        }
      };
    })(this));
    return this;
  };


  /**
   * Match documents using $elemMatch
  #
   * @param {String} key
   * @param {Object} value
   * @api public
   */

  Where.prototype.matches = function(key, value) {
    if (this.lastKey) {
      value = key;
      key = this.lastKey;
      this.lastKey = null;
    }
    this.query[key] = {
      $elemMatch: value
    };
    return this;
  };

  Where.prototype.match = function() {
    return this.matches.apply(this, arguments);
  };


  /**
   * Between
  #
   * @param {String} key - key
   * @param {Mixed} value - value
   * @api public
   */

  Where.prototype.between = function(key, arg) {
    var gte, lte;
    gte = arg[0], lte = arg[1];
    this.lastKey = key;
    this.gte(gte);
    this.lastKey = key;
    this.lte(lte);
    return this;
  };


  /**
   * Same as .where(), only less flexible
  #
   * @param {String} key - key
   * @param {Mixed} value - value
   * @api public
   */

  Where.prototype.inq = function(key, value) {
    return this["in"](key, value);
  };

  Where.prototype.neq = function(key, value) {
    return this.ne(key, value);
  };

  Where.prototype.equals = function(value) {
    var key;
    key = this.lastKey;
    this.lastKey = null;
    this.query[key] = value;
    return this;
  };


  /**
   * Set property that must or mustn't exist in resulting docs
  #
   * @param {String} key - key
   * @param {Boolean} exists - exists or not
   * @api public
   */

  Where.prototype.exists = function(key, exists) {
    if (exists == null) {
      exists = true;
    }
    if (this.lastKey) {
      exists = key;
      key = this.lastKey;
      this.lastKey = null;
    }
    this.query[key] = {
      $exists: exists
    };
    return this;
  };

  return Where;

})();

['lt', 'lte', 'gt', 'gte', 'in', 'nin', 'ne'].forEach(function(method) {
  Where.prototype[method] = function(key, value) {
    var base, hasValue, operator;
    if (this.lastKey) {
      value = key;
      key = this.lastKey;
      this.lastKey = null;
    }
    operator = '$' + method;
    hasValue = value !== void 0;
    if (hasValue) {
      if ((base = this.query)[key] == null) {
        base[key] = {};
      }
      this.query[key][operator] = value;
    } else {
      this.query[operator] = key;
    }
    return this;
  };
});

['or', 'nor', 'and'].forEach(function(method) {
  Where.prototype[method] = function() {
    var args, operator;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    operator = '$' + method;
    this.query[operator] = args;
    return this;
  };
});

module.exports = Where;
