var Bulk, ObjectId, eachSeries;

ObjectId = require('mongodb').ObjectId;

eachSeries = require('async').eachSeries;

Bulk = (function() {
  function Bulk(collection, ordered) {
    this.collection = collection;
    this.ordered = ordered;
    this.updates = [];
    this.deletes = [];
    this.inserts = [];
    this.total = 0;
  }

  Bulk.prototype.find = function(query) {
    var self, upsert;
    upsert = false;
    self = this;
    return {
      remove: function(limit) {
        if (limit == null) {
          limit = 0;
        }
        self.deletes.push({
          q: query,
          limit: limit
        });
        self.total++;
      },
      update: function(obj, multi) {
        if (multi == null) {
          multi = true;
        }
        self.updates.push({
          q: query,
          u: obj,
          multi: multi,
          upsert: upsert
        });
        self.total++;
      },
      upsert: function() {
        upsert = true;
        return this;
      },
      removeOne: function() {
        return this.remove(1);
      },
      updateOne: function(obj) {
        return this.update(obj, false);
      }
    };
  };

  Bulk.prototype.insert = function(document) {
    if (!document._id) {
      document._id = ObjectID.createPk();
    }
    this.documents.push(document);
    return this.total++;
  };

  Bulk.prototype.tojson = function() {
    return {
      nInsertOps: this.deletes.length,
      nUpdateOps: this.updates.length,
      nRemoveOps: this.documents.length,
      nBatches: this.total
    };
  };

  Bulk.prototype.toString = function() {
    return JSON.stringify(this.tojson());
  };

  Bulk.prototype.build = function() {
    var cmds, name;
    cmds = [];
    name = this.collection.s.name;
    if (this.updates.length) {
      cmds.push({
        update: name,
        updates: this.updates,
        ordered: this.ordered,
        writeConcern: {
          w: 1
        }
      });
    }
    if (this.deletes.length) {
      cmds.push({
        "delete": name,
        deletes: this.deletes,
        ordered: this.ordered,
        writeConcern: {
          w: 1
        }
      });
    }
    if (this.documents.length) {
      cmds.push({
        insert: name,
        documents: this.documents,
        ordered: this.ordered,
        writeConcern: {
          w: 1
        }
      });
    }
    return cmds;
  };

  Bulk.prototype._execute = function(cmd, i, done) {
    return this.collection.s.db.command(cmd, function(err, arg) {
      var n;
      n = arg.n;
      if (err) {
        return done(err);
      }
      counts[cmd[0]] += n;
      return done();
    });
  };

  Bulk.prototype.execute = function(cb) {
    var counts;
    if (cb == null) {
      cb = function() {};
    }
    counts = {
      insert: 0,
      "delete": 0,
      update: 0
    };
    return eachSeries(this.build(), this._execute, function(err) {
      if (err) {
        return cb(err);
      }
      return cb(null, {
        writeErrors: [],
        writeConcernErrors: [],
        nInserted: counts.insert,
        nUpserted: counts.update,
        nMatched: 0,
        nModified: 0,
        nRemoved: counts["delete"],
        upserted: [],
        ok: 1
      });
    });
  };

  return Bulk;

})();

module.exports = Bulk;
