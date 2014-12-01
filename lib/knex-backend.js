'use strict';

var log4js = require('log4js');
var logger = log4js.getLogger('node-acl-knex');

var knex = require('knex');
var async = require('async');
var _ = require('lodash');

function KnexDBBackend(db){
  this.db = db;
}

var BUCKET_META = 0;
var BUCKET_RESOURCES = 1;
var BUCKET_PARENTS = 2;
var BUCKET_USERS = 3;
var BUCKET_PERMISSIONS = 4;

var bucketNumbers = {
  'meta': BUCKET_META,
  'resources': BUCKET_RESOURCES,
  'parents': BUCKET_PARENTS,
  'users': BUCKET_USERS,
  'permissions': BUCKET_PERMISSIONS
};

function normalizeRequest(request) {

}

KnexDBBackend.prototype = {

  normalizeUpdate: function(update) {
    if (update.bucket.indexOf('allows') != -1) {
      update.searchBucket = bucketNumbers['permissions'];
      update.searchKey = update.bucket;
      update.getFn = function(update, old) {
        old = (old === undefined) ? {} : old;
        if (old.hasOwnProperty(update.key))
          return old[update.key];
        else 
          return [];
      };
      update.updateFn = function(update, old) {
        var values = update.values;
        old = (old === undefined) ? {} : old;
        values = Array.isArray(values) ? values : [values];
        var oldValues = old[update.key]
        old[update.key] = _.union(values, oldValues)
        return old;
      };
      update.removeFn = function(update, old) {
        var values = update.values;
        values = Array.isArray(values) ? values : [values];
        _.each(values, function(value) {
          old[update.key] = _.without(old[update.key], value);
        });
        if (old[update.key].length === 0) {
          old = _.omit(old, update.key);
        }
        return old;
      };
    } else {
      update.searchBucket = bucketNumbers[update.bucket];
      update.searchKey = update.key;
      update.getFn = function(update, old) {
        return (old === undefined) ? [] : old;
      };
      update.updateFn = function(update, old) {
        var values = update.values;
        old = (old === undefined) ? [] : old;
        values = Array.isArray(values) ? values : [values];
        return _.union(values, old);
      };
      update.removeFn = function(update, old) {
        var values = update.values;
        values = Array.isArray(values) ? values : [values];
        _.each(values, function(value) {
          old = _.without(old, value);
        });
        return old;
      };
    }
    return update;
  },

  begin: function() {
    return {promise: this.db.raw('savepoint service_transaction')};
  },

  add: function(transaction, bucket, key, values) {

    var update = this.normalizeUpdate({bucket: bucket, key: key, values: values});
    var _this = this;

    transaction.promise = transaction.promise.then(function() {
      return _this.db('acl').first('id', 'key', 'values')
        .where({bucket: update.searchBucket, key: update.searchKey})
        .then(function(found) {
          var data = undefined;
          if (found !== undefined) {
            data = JSON.parse(found.values);
          } else {
          }
          var newData = update.updateFn(update, data);
          if (found !== undefined) {
            return _this.db('acl').where('id', '=', found.id).update({values: JSON.stringify(newData)});
          } else {
            return _this.db('acl').insert({bucket: update.searchBucket, key: update.searchKey, values: JSON.stringify(newData)})
          }
        })
      });
  },

  del: function(transaction, bucket, keys) {
    var _this = this;
    var update = this.normalizeUpdate({bucket: bucket, key: keys});
    if (update.searchBucket === BUCKET_PERMISSIONS) {
      transaction.promise = transaction.promise.then(function() {
        return _this.db.first('key', 'values')
          .from('acl')
          .where({'bucket': update.searchBucket, 'key': update.searchKey})
          .then(function(result) {
            if (result !== undefined) {
              var value = JSON.parse(result.values);
              var keys = update.key;
              keys = Array.isArray(keys) ? keys : [keys];
              _.each(keys, function(k) {
                value = _.omit(value, k);
              });
              if (_.isEmpty(value)) {
                return _this.db('acl')
                  .where({'bucket': update.searchBucket, 'key': update.searchKey})
                  .delete()
              } else {
                return _this.db('acl')
                  .where({'bucket': update.searchBucket, 'key': update.searchKey})
                  .update({values: JSON.stringify(value)})
              }
            }
          });
        });
    } else {
      transaction.promise = transaction.promise.then(function() {
        return _this.db('acl')
          .where({'bucket': update.searchBucket})
          .andWhere('key', 'in', update.searchKey)
          .delete()
      });
    }
  },

  remove: function(transaction, bucket, key, values) {
    var update = this.normalizeUpdate({bucket: bucket, key: key, values: values});

    var _this = this;

    transaction.promise = transaction.promise.then(function() {
      return _this.db('acl').first('id', 'key', 'values')
        .where({bucket: update.searchBucket, key: update.searchKey})
        .then(function(found) {
          var data = undefined;
          if (found !== undefined) {
            data = JSON.parse(found.values);
          } else {
          }
          var newData = update.removeFn(update, data);
          if (found !== undefined) {
            return _this.db('acl').where('id', '=', found.id).update({values: JSON.stringify(newData)});
          }
        }).then(function(result) {
        }).catch(function(err) {
          logger.error("Got error", err);
        });
      });
  },

  clean: function(callback) {
    logger.error("Called clean");
    var _this = this;
    return _this.db.truncate('acl')
      .then(function() {
        return callback(null);
      }).catch(function(err) {
        return callback(err);
      });
  },

  end: function(transaction, callback){
    var _this = this;
    return transaction.promise.then(function() {
      return _this.db.raw('release savepoint service_transaction')
        .then(function() {
          return callback(null);
        });
    }).catch(function(err) {
      return _this.db.raw('rollback to service_transaction').then(function() {
        return _this.db.raw('release savepoint service_transaction');
      }).then(function() {
        return callback(err);
      });
    });
  },

  get: function(bucket, key, callback) {
    var update = this.normalizeUpdate({bucket: bucket, key: key});
    return this.db.first('key', 'values')
      .from('acl')
      .where({'bucket': update.searchBucket, 'key': update.searchKey})
      .then(function(value) {
        if (value !== undefined) {
          value = JSON.parse(value.values);
        }
        callback(null, update.getFn(update, value));
      }).catch(function(err) {
        callback(err, null);
      });
  },

  union: function(bucket, keys, cb) {
    var update = this.normalizeUpdate({bucket: bucket, key: keys});

    if (update.searchBucket === BUCKET_PERMISSIONS) {


      return this.db.first('key', 'values')
        .from('acl')
        .where({'bucket': update.searchBucket, 'key': update.searchKey})
        .then(function(value) {
          if (value !== undefined) {
            value = JSON.parse(value.values);
          } else {
            value = {};
          }
          var keyArrays = [];
          _.each(update.key, function(key) {
            keyArrays.push.apply(keyArrays, value[key]);
          });
          var result = _.union(keyArrays);
          return cb(undefined, result);
        });

    } else {

      return this.db.select('id', 'key', 'values')
        .from('acl')
        .where({'bucket': update.searchBucket})
        .andWhere('key', 'in', update.searchKey)
        .then(function(value) {
          var keyArrays = [];
          if (value.length) {
            _.each(value, function(result) {
              value = JSON.parse(result.values);
              keyArrays.push.apply(keyArrays, value);
            });
          }
          var result = _.union(keyArrays);
          return cb(undefined, result);
        });
    }
  },

  setup: function(callback) {
    var _this = this;
    _this.db.schema.hasTable('acl').then(function(exists) {
      if (!exists) {
        return _this.db.schema.createTable('acl', function(table) {
          table.increments('id');
          table.integer('bucket').notNullable();
          table.string('key', 24).notNullable();
          table.text('values', 'longtext').notNullable();
          table.unique(['bucket', 'key']);
        });
      }
    }).then(function() {
      return callback(null);
    }).catch(function(err) {
      return callback(err);
    });
  },

  teardown: function(callback) {
    this.db.schema.dropTable('acl')
      .then(function() {
        return callback(null);
      }).catch(function(err) {
        return callback(err);
    });
  }
}

exports = module.exports = KnexDBBackend;