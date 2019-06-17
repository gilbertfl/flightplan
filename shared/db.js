const sql = require('mssql')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const util = require('util');
const paths = require('./paths')
const prompts = require('../shared/prompts')

let _pool = null;


// Create connection to database
var connectionConfig =
{
  user: paths.databaseUser, 
  password: paths.databasePassword, 
  server: paths.database, 
  database: paths.databaseName, 
  encrypt: true
};

sql.on('error', err => {
  console.error(err);
})

async function open () {
  if (!_pool) {
    console.log(`Attempting to open database pool: ${paths.database}`);
    _pool = await sql.connect(connectionConfig);
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
  var result = await _pool.request()
    .input('requestId', sql.Int, requestId)
    .query('DELETE FROM requests WHERE id = @requestId');
  return result.recordset;
}

async function saveSegment(row) {
  const transaction = _pool.transaction();
  let success = false;
  const idToReturn = null;

  transaction.begin(async err => {

    transaction.on('rollback', aborted => {
      console.error("save segment transaction rolled back", aborted);
    });

    try {
      const entries = Object.entries(row);
      const colNames = entries.map(x => x[0]);
      const colVals = entries.map(x => coerceType(x[1]));
      var request = transaction.request();
      await request.query(`INSERT segments (${colNames.join(',')}) OUTPUT INSERTED.id VALUES (${colVals.map(x => '?').join(',')});`, (err, result) => {
        if (err) {
          success = false;
        } else {
          // TODO: how to get idToReturn?
          console.log("segments successfully inserted, do something with result", result);
        }
      });
    } finally {
      if (success) {
        transaction.commit(tErr => tErr && next('transaction commit error'));
      } else { 
        transaction.rollback(err => {
          console.error("Unable to roll back on error during sql transaction!");
        });
      }
    }

    return success ? idToReturn : null;
  });

  return transaction;
}

async function saveRequest(row) {
  const transaction = _pool.transaction();
  let success = false;
  const idToReturn = null;

  transaction.begin(async err => {

    transaction.on('rollback', aborted => {
      console.error("save request transaction rolled back", aborted);
    });

    try {
      const entries = Object.entries(row);
      const colNames = entries.map(x => x[0]);
      const colVals = entries.map(x => coerceType(x[1]));
      var request = transaction.request();
      await request.query(`INSERT requests (${colNames.join(',')}) OUTPUT INSERTED.id VALUES (${colVals.map(x => '?').join(',')});`, (err, result) => {
        if (err) {
          success = false;
        } else {
          // TODO: how to get idToReturn?
          console.log("get result somehow", result);
        }
      });
    } finally {
      if (success) {
        transaction.commit(tErr => tErr && next('transaction commit error'));
      } else { 
        transaction.rollback(err => {
          console.error("Unable to roll back on error during sql transaction!");
        });
      }
    }

    return success ? idToReturn : null;
  });

  return transaction;
}

async function saveAwards(requestId, rows) {
  // Wrap everything in a transaction
  const transaction = _pool.transaction();
  
  transaction.begin(async err => {

    let success = true;
    const saveAwardResults = [];
    const ids = [];

    transaction.on('rollback', aborted => {
      console.error("save awards transaction rolled back", aborted);
      success = false;
    });

    try {
      for (const row of rows) {
        const { segments } = row;
        delete row.segments;

        // Save the individual award and get it's ID
        row.requestId = requestId;

        const entries = Object.entries(row);
        const colNames = entries.map(x => x[0]);
        const colVals = entries.map(x => coerceType(x[1]));
        const innerRequest = transaction.request();

        saveAwardResults.push(await innerRequest.query(`INSERT awards (${colNames.join(',')}) OUTPUT INSERTED.id VALUES (${colVals.map(x => '?').join(',')});`));
        //   , (err, result) => {
        //   if (err) {
        //     success = false;
        //   } else {
        //     // TODO: how to get awardid?
        //     console.log("somehow get award id", result);

        //     ids.push(awardId);

        //     // Now add each segment
        //     if (segments) {
        //       segments.forEach((segment, position) => {
        //         saveSegment(awardId, position, segment)
        //       })
        //     }
        //   }
        // });
      }
      transaction.commit(err => {
          console.error("transaction commit failed, rolling back.");
          success = false;
      });
    } catch {
      transaction.rollback(err => {
        if (err) {
          console.error("unhandled exception, and could not roll back!");
        } else {
          console.error("unhandled exception, rolled back.");
        }
      });
      success = false;
    }

    if (success) {
      for (const saveAwardResult of saveAwardResults) {

        console.log(saveAwardResult);

        ids.push(awardId);

        // Now add each segment
        if (segments) {
          segments.forEach((segment, position) => {
            saveSegment(awardId, position, segment)
          })
        }
      }
    }

    return success ? ids : null;
  });

  return transaction;
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


module.exports = {
  getAllRequests, 
  getRequestsForRT, 
  getRequestsForOW, 
  getAllAwards, 
  getAwardsForRT,
  getAwardsForOW, 
  open,
  saveAwards, 
  saveSegment, 
  saveRequest, 
  coerceType,
  count,
  cleanupRequest, 
  close 
}
