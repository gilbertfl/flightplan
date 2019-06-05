var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var ConnectionPool = require('tedious-connection-pool');

const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const util = require('util');
const paths = require('./paths')
const prompts = require('../shared/prompts')

//let _db = null
let _pool = null

function db () {
  return _db
}


// Create connection to database
var connectionConfig =
{
    authentication: {
        options: {
            userName: paths.databaseUser,
            password: paths.databasePassword
        },
        type: 'default'
    },
    server: paths.database, 
    options:
    {
        database: paths.databaseName,
        encrypt: true, 
        rowCollectionOnRequestCompletion: true // <-- allows rows in request callback as per https://tediousjs.github.io/tedious/api-request.html
    }
}
var poolConfig = {
  min: 2,
  max: 4,
  log: true
};

async function open () {
  if (!_pool) {
    console.log(`Attempting to open database pool: ${paths.database}`);


    //create the pool
    _pool = new ConnectionPool(poolConfig, connectionConfig);

    pool.on('error', function(err) {
        console.error(err);
    });
  
    // _db = new Connection(config);
    
    // // wrap database event callbacks into a promise
    // _db.on[util.promisify.custom] = (eventName) => {
    //   return new Promise((resolve, reject) => {
    //      _db.on(eventName, function(err) {
    //        if (err) {
    //          console.error(`error on ${eventName}`, err);
    //          reject(err);
    //        } else {
    //          resolve();
    //        }
    //      });
    //   });
    // };
    // const dbOnEventFunction = util.promisify(_db.on);

    // // don't return until the db is connected (or until it's failed)
    // await dbOnEventFunction('connect');
  }

  return _db;
}

// override promise generation so we can return *both* rowcount and rows
function createPromiseFromRequest(db, sql, requestFunction) {
  return new Promise((resolve, reject) => {
    requestFunction(db, sql, function(err, rowCount, rows) {
      if (err) {
        reject(err);
      } else {
        resolve({ rowCount, rows });
      }
    });
  });
}

async function insertRow (table, row) {
  const entries = Object.entries(row)
  const colNames = entries.map(x => x[0])
  const colVals = entries.map(x => coerceType(x[1]))
  
  //const sql = `INSERT INTO ${table} (${colNames.join(',')}) VALUES (${colVals.map(x => '?').join(',')})`
  const sql = `INSERT ${table} (${colNames.join(',')}) OUTPUT INSERTED.id VALUES (${colVals.map(x => '?').join(',')});`

  var oneWayFn = function (db, sql, requestCallback) {
    var request = new Request(sql, requestCallback);  
    request.addParameter('engine', TYPES.NVarChar, engine);
    request.addParameter('cabin', TYPES.NVarChar, cabin);
    request.addParameter('quantity', TYPES.NVarChar, quantity);
    request.addParameter('fromCity', TYPES.NVarChar, fromCity);
    request.addParameter('toCity', TYPES.NVarChar, toCity);
    request.addParameter('departStr', TYPES.NVarChar, departStr);
    db.execSql(request); 
  };
  oneWayFn[util.promisify.custom] = (innerdb, innersql) => createPromiseFromRequest(innerdb, innersql, oneWayFn);
  const oneWayPromise = util.promisify(oneWayFn);
  var result = await oneWayPromise(_db, sql);
  return result.rows;
}

function coerceType (val) {
  if (typeof val === 'boolean') {
    return val ? 1 : 0
  }
  return val
}

function count (table) {
  const sql = `SELECT count(*) FROM ${table}`;
  
  //const result = _db.prepare(sql).get()
  var request = new Request(sql, function(err, rowCount, rows) {  
    if (err) {  
      console.error(err);
    }  
  });
  _db.execSql(request);

  return result ? result['count(*)'] : undefined
}

function close () {
  if (_db) {
    _db.close()
    _db = null
  }
}

function begin () {
  //_db.prepare('BEGIN').run()
  _db.beginTransaction(err => {
    if (err) {
      console.error('begin transaction error', err)
    }
  });
}

function commit () {
  //_db.prepare('COMMIT').run()
  _db.commitTransaction(err => {
    if (err) {
      console.error('commit transaction error', err)
    }
  });
}

function rollback () {
  //_db.prepare('ROLLBACK').run()
  _db.rollbackTransaction(err => {
    if (err) {
      console.error('rollback transaction error', err)
    }
  });
}

module.exports = {
//  db,
  open,
  insertRow,
  coerceType,
  count,
  close,
  begin,
  commit,
  rollback, 
  createPromiseFromRequest
}
