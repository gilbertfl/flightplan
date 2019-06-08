const sql = require('mssql')

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


// Create connection to database
var connectionConfig =
{
  user: paths.databaseUser, 
  password: paths.databasePassword, 
  server: paths.database, 
  database: paths.databaseName, 
  encrypt: true
    // authentication: {
    //     options: {
    //         userName: paths.databaseUser,
    //         password: paths.databasePassword
    //     },
    //     type: 'default'
    // },
    // server: paths.database, 
    // options:
    // {
    //     database: paths.databaseName,
    //     encrypt: true, 
    //     rowCollectionOnRequestCompletion: true // <-- allows rows in request callback as per https://tediousjs.github.io/tedious/api-request.html
    // }
};

sql.on('error', err => {
  console.error(err);
})

async function open () {
  if (!_pool) {
    console.log(`Attempting to open database pool: ${paths.database}`);

    //create the pool
    _pool = await sql.connect(connectionConfig)
  }

  return _pool;
}

async function getRequestsForOW(route) {
  
  // Format dates
  const { engine, partners, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null

  var result = await _pool.request()
    .input('engine', sql.VarChar, engine)
    .input('partners', sql.Bit, partners ? 1 : 0)
    .input('cabin', sql.VarChar, cabin)
    .input('quantity', sql.Int, quantity)
    .input('fromCity', sql.VarChar, fromCity)
    .input('toCity', sql.VarChar, toCity)
    .input('departStr', sql.VarChar, departStr)
    .query('SELECT * FROM requests WHERE ' +
        'engine = @engine AND partners = @partners AND cabin = @cabin AND quantity = @quantity AND (' +
        '(fromCity = @fromCity AND toCity = @toCity AND departDate = @departStr) OR ' +
        '(fromCity = @toCity AND toCity = @fromCity AND returnDate = @departStr))');

  return result.recordset;
}

async function getRequestsForRT(route) {
  // Format dates
  const { engine, partners, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null
  const returnStr = returnDate || null

  var result = await _pool.request()
    .input('engine', sql.VarChar, engine)
    .input('partners', sql.Bit, partners ? 1 : 0)
    .input('cabin', sql.VarChar, cabin)
    .input('quantity', sql.Int, quantity)
    .input('fromCity', sql.VarChar, fromCity)
    .input('toCity', sql.VarChar, toCity)
    .input('departStr', sql.VarChar, departStr)
    .input('returnStr', sql.VarChar, returnStr)
    .query('SELECT * FROM requests WHERE ' +
    'engine = @engine AND partners = @partners AND cabin = @cabin AND quantity = @quantity AND (' +
    '(fromCity = @fromCity AND toCity = @toCity AND (departDate = @departStr OR returnDate = @returnStr)) OR ' +
    '(fromCity = @toCity AND toCity = @fromCity AND (departDate = @returnStr OR returnDate = @departStr)))');

    return result.recordset;
}

async function getAwardsForRT(route) {
  // Format dates
  const { engine, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null
  const returnStr = returnDate || null

  var result = await _pool.request()
    .input('engine', sql.VarChar, engine)
    .input('cabin', sql.VarChar, cabin)
    .input('quantity', sql.Int, quantity)
    .input('fromCity', sql.VarChar, fromCity)
    .input('toCity', sql.VarChar, toCity)
    .input('departStr', sql.VarChar, departStr)
    .input('returnStr', sql.VarChar, returnStr)
    .query( 'SELECT * FROM awards WHERE ' +
       'engine = @engine AND cabin = @cabin AND quantity <= @quantity AND (' +
       '(fromCity = @fromCity AND toCity = @toCity AND date = @departStr) OR ' +
       '(fromCity = @toCity AND toCity = @fromCity AND date = @returnStr))');

  return result.recordset;
}

async function getAwardsForOW(route) {
  // Format dates
  const { engine, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null

  var result = await _pool.request()
    .input('engine', sql.VarChar, engine)
    .input('cabin', sql.VarChar, cabin)
    .input('quantity', sql.Int, quantity)
    .input('fromCity', sql.VarChar, fromCity)
    .input('toCity', sql.VarChar, toCity)
    .input('departStr', sql.VarChar, departStr)
    .query('SELECT * FROM awards WHERE ' +
      'engine = @engine AND cabin = @cabin AND quantity <= @quantity AND ' +
      'fromCity = @fromCity AND toCity = @toCity AND date = @departStr');

  return result.recordset;
}

async function getAllRequests() {
  var result = await _pool.request()
    .query('SELECT * FROM requests');
  return result.recordset;
}

async function getAllAwards() {
  var result = await _pool.request()
    .query('SELECT * FROM awards');
  return result.recordset;
}

async function cleanupRequest(requestId) {
  //db.db().prepare('DELETE FROM requests WHERE id = ?').run(request.id)
  var result = await _pool.request()
    .input('requestId', sql.Int, requestId)
    .query('DELETE FROM requests WHERE id = @requestId');
  return result.recordset;
}

async function insertRow (table, row) {
  const entries = Object.entries(row)
  const colNames = entries.map(x => x[0])
  const colVals = entries.map(x => coerceType(x[1]))

  var sqlStr = `INSERT ${table} (${colNames.join(',')}) OUTPUT INSERTED.id VALUES (${colVals.map(x => '?').join(',')});`;
  var result = await _pool.request()
    .query(sqlStr);

  return result.recordset;
}

function coerceType (val) {
  if (typeof val === 'boolean') {
    return val ? 1 : 0
  }
  return val
}

async function count (table) {
  const sqlStr = `SELECT count(*) FROM ${table}`;
  
  const result = await _pool.request()
    .query(sqlStr);

  return result ? result['count(*)'] : undefined
}

function close () {
  if (_pool) {
    _pool.close();
    _pool = null
  }
}

// function begin () {
//   //_db.prepare('BEGIN').run()
//   const transaction = _pool.transaction();


//   _db.beginTransaction(err => {
//     if (err) {
//       console.error('begin transaction error', err)
//     }
//   });
// }

// function commit () {
//   //_db.prepare('COMMIT').run()
//   _db.commitTransaction(err => {
//     if (err) {
//       console.error('commit transaction error', err)
//     }
//   });
// }

// function rollback () {
//   //_db.prepare('ROLLBACK').run()
//   _db.rollbackTransaction(err => {
//     if (err) {
//       console.error('rollback transaction error', err)
//     }
//   });
// }

module.exports = {
//  db,
  getAllRequests, 
  getRequestsForRT, 
  getRequestsForOW, 
  getAllAwards, 
  getAwardsForRT,
  getAwardsForOW, 
  open,
  insertRow,
  coerceType,
  count,
  cleanupRequest, 
  close //,
  //begin,
  //commit,
  //rollback, 
  //createPromiseFromRequest
}
