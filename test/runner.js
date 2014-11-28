'use strict';

var knex = require('knex');
var KnexBackend = require('../lib/knex-backend');
var tests = require('../node_modules/acl/test/tests');
var assert = require('chai').assert;
var error = null;

function run() {
  Object.keys(tests).forEach(function (test) {
    tests[test]();
  });
}

describe('SQLite', function () {

  describe('testing setup method', function () {

    before(function () {
      error = null;
    });
    
    describe('with passing db', function () {
      before(function (done) {
        var self = this;
        var db = knex({
          client: 'sqlite',
          connection: {filename: 'test.db'}
        });
        new KnexBackend(db).setup(function(err) {
          error = err;
          if (err) return done(err);
          done();
        });
      });
      
      it('should create tables in database', function () {
        assert(!error);
      });
      
      describe('and then using teardown method', function () {
        before(function (done) {
          var self = this;
          var db = knex({
            client: 'sqlite',
            connection: {filename: 'test.db'}
          });
          new KnexBackend(db).teardown(function(err) {
            error = err;
            if (err) return done(err);
            done();
          });
        });
        
        it('should drop tables in database', function () {
          assert(!error);
        });
      });
    });
  });
    
  describe('Acl Test', function () {
    before(function (done) {
      var self = this;
      var db = knex({
        client: 'sqlite',
        connection: {filename: 'test.db'}
      });
      new KnexBackend(db).setup(function(err) {
        if (err) return done(err);
        self.backend = new KnexBackend(db);
        done();
      });
    });
    
    run();
  });
});

// Mysql and SQLite support coming soon.

// describe('MySql', function () {
//  before(function (done) {
//    var self = this;
//    var db = knex({
//      client: 'mysql',
//      connection: {
//        host: '127.0.0.1',
//        port: 3306,
//        user: 'root',
//        password: ''
//        database: 'travis_ci_test'
//      }
//    });
    
//    var downSql = 'DROP TABLE IF EXISTS acl_meta, acl_resources, acl_parents, acl_users, acl_permissions';
    
//    db.raw(downSql)
//      .then(function() {
//        self.backend = new KnexBackend(db, 'mysql', 'acl_');
//        done();
//      })
//    ;
    
//  });
  
//  run();
// });

// describe('SQLite', function () {
//  before(function (done) {
//    var self = this;
//    var db = knex({
//      client: 'sqlite',
//      connection: {
//        filename: './travis_ci_test.sqlite'
//      }
//    });
    
//  });
  
//  run();
// });