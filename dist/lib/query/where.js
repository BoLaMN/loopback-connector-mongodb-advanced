'use strict';
var Where, debug, isArray, isObject, isRegExp, isString, isUndefined, ref,
  slice = [].slice;

debug = require('debug')('loopback:connector:mongodb-advanced');

ref = require('lodash'), isObject = ref.isObject, isString = ref.isString, isArray = ref.isArray, isUndefined = ref.isUndefined, isRegExp = ref.isRegExp;


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

  Where.prototype.parse = function(conditions, value) {
    var keys;
    if (isObject(conditions)) {
      keys = Object.keys(conditions);
      keys.forEach((function(_this) {
        return function(key) {
          return _this.parse(key, conditions[key]);
        };
      })(this));
    }
    if (isString(conditions)) {
      if (isUndefined(value)) {
        this.lastKey = conditions;
        return this;
      }
      if (isRegExp(value)) {
        value = {
          $regex: value
        };
      }
      if (isArray(value)) {
        value = {
          $in: value
        };
      }
      if (conditions === 'id') {
        conditions = '_id';
      }
      this.query[conditions] = value;
    }
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
   * Same as .where(), only less flexible
  #
   * @param {String} key - key
   * @param {Mixed} value - value
   * @api public
   */

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
    var hasValue, operator;
    if (this.lastKey) {
      value = key;
      key = this.lastKey;
      this.lastKey = null;
    }
    operator = '$' + method;
    hasValue = value !== void 0;
    if (hasValue) {
      this.query[key] = {};
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
