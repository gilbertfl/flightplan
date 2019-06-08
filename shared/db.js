var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var ConnectionPool = require('tedious-connection-pool');
var TYPES = require('tedious').TYPES;

const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const util = require('util');
const paths = require('./paths')
const prompts = require('../shared/prompts')

//let _db = null
let _pool = null

// function db () {
//   return _db
// }

// */
// server?: string;

// /**
//  * Once you set domain, driver will connect to SQL Server using domain login.
//  */
// domain?: string;

// /**
//  * Further options
//  */
// options?: ConnectionOptions;

// /**
//  * Authentication Options
//  */
// authentication?: ConnectionAuthentication;

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
};
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

    _pool.on('error', function(err) {
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

  return _pool;
}

// override promise generation so we can return *both* rowcount and rows
function createPromiseFromRequest(db, param, requestFunction) {
  return new Promise((resolve, reject) => {
    requestFunction(db, param, function(err, rowCount, rows) {
      if (err) {
        reject(err);
      } else {
        resolve({ rowCount, rows });
      }
    });
  });
}

function acquireConnection(pool) {
  return new Promise((resolve, reject) => {
     pool.acquire(function (err, database) {
      if (err) {
        console.error(err);
        reject(err);
      } else {
        resolve(database);
      }
    });
  });
}
acquireConnection[util.promisify.custom] = (innerpool) => acquireConnection(innerpool);

async function getRequestsForOW(route) {
  
  var database = await acquireConnection(_pool);

  var oneWayFn = function (db, route, requestCallback) {
    // Format dates
    const { engine, partners, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
    const departStr = departDate || null
    const returnStr = returnDate || null

    // One-Way route
    const sql = 'SELECT * FROM requests WHERE ' +
    'engine = @engine AND partners = @partners AND cabin = @cabin AND quantity = @quantity AND (' +
    '(fromCity = @fromCity AND toCity = @toCity AND departDate = @departStr) OR ' +
    '(fromCity = @toCity AND toCity = @fromCity AND returnDate = @departStr))'

    var request = new Request(sql, requestCallback);  
    request.addParameter('engine', TYPES.VarChar, engine);
    request.addParameter('partners', TYPES.Bit, partners ? 1 : 0);
    request.addParameter('cabin', TYPES.VarChar, cabin);
    request.addParameter('quantity', TYPES.Int, quantity);
    request.addParameter('fromCity', TYPES.VarChar, fromCity);
    request.addParameter('toCity', TYPES.VarChar, toCity);
    request.addParameter('departStr', TYPES.VarChar, departStr);
    db.execSql(request);
  };
  oneWayFn[util.promisify.custom] = (innerdb, innerroute) => createPromiseFromRequest(innerdb, innerroute, oneWayFn);
  const getRequestsFn = util.promisify(oneWayFn);
  var result = await getRequestsFn(database, route);
  var rows = result.rows;
  database.release();
  return rows;
}

async function getRequestsForRT(route) {
  // Format dates
  const { engine, partners, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null
  const returnStr = returnDate || null

  // Round-Trip route
  const sql = 'SELECT * FROM requests WHERE ' +
      'engine = @engine AND partners = @partners AND cabin = @cabin AND quantity = @quantity AND (' +
      '(fromCity = @fromCity AND toCity = @toCity AND (departDate = @departStr OR returnDate = @returnStr)) OR ' +
      '(fromCity = @toCity AND toCity = @fromCity AND (departDate = @returnStr OR returnDate = @departStr)))'
  
  _pool.acquire(async function (err, database) {
    if (err) {
        console.error(err);
        return;
    }
    var roundTripFn = function (db, sql, requestCallback) {
      var request = new Request(sql, requestCallback);  
      request.addParameter('engine', TYPES.VarChar, engine);
      request.addParameter('partners', TYPES.Bit, partners ? 1 : 0);
      request.addParameter('cabin', TYPES.VarChar, cabin);
      request.addParameter('quantity', TYPES.Int, quantity);
      request.addParameter('fromCity', TYPES.VarChar, fromCity);
      request.addParameter('toCity', TYPES.VarChar, toCity);
      request.addParameter('departStr', TYPES.VarChar, departStr);
      request.addParameter('returnStr', TYPES.VarChar, returnStr);
      db.execSql(request);
    };
    roundTripFn[util.promisify.custom] = (innerdb, innersql) => createPromiseFromRequest(innerdb, innersql, roundTripFn);
    const getRequestsFn = util.promisify(roundTripFn);
    var result = await getRequestsFn(database, sql);
    var rows = result.rows;
    database.release();
    return rows;
  });
}

async function getAllRequests() {
  _pool.acquire(async function (err, database) {
    if (err) {
        console.error(err);
        return;
    }
    var getAllRequestsFn = function (db, sql, requestCallback) {
      var request = new Request(db, requestCallback);
      db.execSql(request);
    };
    getAllRequestsFn[util.promisify.custom] = (innerdb, innersql) => 
      db.createPromiseFromRequest(innerdb, innersql, getAllRequestsFn);
    const getAllRequestsPromise = util.promisify(getAllRequestsFn);
    var allRoutesResult = await getAllRequestsPromise(database, 'SELECT * FROM requests');
    console.log("all routes result", allRoutesResult);
    var rows = allRoutesResult.rows;
    database.release();
    return rows;
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
  getAllRequests, 
  getRequestsForRT, 
  getRequestsForOW, 
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
