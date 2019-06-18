const sql = require('mssql')
const util = require('util')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
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
  encrypt: true, 
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

async function open () {
  if (!_pool) {
    console.log(`Attempting to open database pool: ${paths.database}`);
    //_pool = await sql.connect(connectionConfig);
    _pool = new sql.ConnectionPool(connectionConfig);
    await _pool.connect();

    _pool.on('error', err => {
      console.error(err);
    })
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
  //let success = false;
  
  transaction.begin(async transactionBeginErr => {

    if (transactionBeginErr) {
      console.error("save segment transaction failed to begin.", transactionBeginErr);
    } else {
      try {
        await transaction.request()
            .input('awardId', sql.Int, awardId)
            .input('position', sql.Int, row.position)
            .input('airline', sql.VarChar, row.airline)
            .input('flight', sql.VarChar, row.flight)
            .input('aircraft', sql.VarChar, row.aircraft)
            .input('fromCity', sql.VarChar, row.fromCity)
            .input('toCity', sql.VarChar, row.toCity)
            .input('date', sql.VarChar, row.date)
            .input('departure', sql.VarChar, row.departure)
            .input('arrival', sql.VarChar, row.arrival)
            .input('duration', sql.Int, row.duration)
            .input('nextConnection', sql.Int, row.nextConnection)
            .input('cabin', sql.VarChar, row.cabin)
            .input('stops', sql.Int, row.stops)
            .input('lagDays', sql.Int, row.lagDays)
            .input('bookingCode', sql.VarChar, row.bookingCode)
            .query('INSERT requests (awardId, position, airline, flight, aircraft, fromCity, toCity, date, departure, arrival, duration, nextConnection, cabin, stops, lagDays, bookingCode) OUTPUT INSERTED.id ' + 
                    'VALUES (@awardId, @position, @airline, @flight, @aircraft, @fromCity, @toCity, @date, @departure, @arrival, @duration, @nextConnection, @cabin, @stops, @lagDays, @bookingCode)', (err, result) => {
              if (err) {
                console.error("query failed", err);
                transaction.rollback(rollbackErr => {
                  if (rollbackErr) {
                    console.error("Save request failed, and failed to roll back!", rollbackErr);
                  } else {
                    console.error("Save request failed, successfully rolled back.");
                  }
                  reject(err);
                });
              } else {
                transaction.commit(commitErr => {
                  if (commitErr) {
                    console.error("transaction commit failed, rolling back.", commitErr);
                    transaction.rollback(rollbackErr => {
                      if (rollbackErr) {
                        console.error("Save request failed, and failed to roll back!", rollbackErr);
                      } else {
                        console.error("Save request failed, successfully rolled back.");
                      }
                      reject(commitErr);
                    });
                  } else {
                    console.log("success", result);
                    var insertedRecordId = result.recordset[0].id;
                    resolve(insertedRecordId);
                  }
                });
              }
            });
      } catch (e) {
        console.error("Unhandled exception while saving segment.", e);
        transaction.rollback(err => {
          console.error("Unable to roll back on error during sql transaction!");
        });
      }
    }
  });
}

function doSaveRequest(transaction, row, resolve, reject) {
  transaction.begin(transactionBeginErr => {
    if (transactionBeginErr) {
      console.error("save request transaction failed to begin.", transactionBeginErr);
      reject(transactionBeginErr);
    } else {
      try {
        transaction.request()
          .input('engine', sql.VarChar, row.engine)
          .input('partners', sql.Bit, row.partners ? 1 : 0)
          .input('cabin', sql.VarChar, row.cabin)
          .input('quantity', sql.Int, row.quantity)
          .input('fromCity', sql.VarChar, row.fromCity)
          .input('toCity', sql.VarChar, row.toCity)
          .input('departStr', sql.VarChar, row.departDate)
          .input('returnStr', sql.VarChar, row.returnDate)
          .input('assets', sql.VarChar, row.assets)
          .query('INSERT requests (engine,partners,fromCity,toCity,departDate,returnDate,cabin,quantity,assets) OUTPUT INSERTED.id ' + 
                  'VALUES (@engine, @partners, @fromCity, @toCity, @departStr, @returnStr, @cabin, @quantity, @assets)', (err, result) => {
            if (err) {
              console.error("query failed", err);
              transaction.rollback(rollbackErr => {
                if (rollbackErr) {
                  console.error("Save request failed, and failed to roll back!", rollbackErr);
                } else {
                  console.error("Save request failed, successfully rolled back.");
                }
                reject(err);
              });
            } else {
              transaction.commit(commitErr => {
                if (commitErr) {
                  console.error("transaction commit failed, rolling back.", commitErr);
                  transaction.rollback(rollbackErr => {
                    if (rollbackErr) {
                      console.error("Save request failed, and failed to roll back!", rollbackErr);
                    } else {
                      console.error("Save request failed, successfully rolled back.");
                    }
                    reject(commitErr);
                  });
                } else {
                  console.log("success", result);
                  var insertedRecordId = result.recordset[0].id;
                  resolve(insertedRecordId);
                }
              });
            }
          });
      } catch (e) {
        console.error("Unhandled exception while saving request.", e);
        //success = false;
        transaction.rollback(rollbackErr => {
          if (rollbackErr) {
            console.error("Save request failed, and failed to roll back!", rollbackErr);
          } else {
            console.error("Save request failed, successfully rolled back.");
          }
          reject(e);
        });
      } 
    }
  });
}
doSaveRequest[util.promisify.custom] = (transaction, row) => {
  return new Promise((resolve, reject) => {
    doSaveRequest(transaction, row, resolve, reject);
  });
};

// returns RequestId on success; null on failure
async function saveRequest(row) {
  // since we need to wait around for the result of the insert, promisify the save method so we can await it
  var promisifiedSaveRequest = util.promisify(doSaveRequest);

  // make a new transaction in the pool and actually save the request!
  const transaction = _pool.transaction();
  var result = await promisifiedSaveRequest(transaction, row);
  return result;
}

// returns nothing, TODO: do in 1 big transaction!!
async function saveAwards(requestId, rows) {

  for (const row of rows) {
    
    const transaction = _pool.transaction();
    
    transaction.begin(async transactionBeginErr => {
      
      if (transactionBeginErr) {
        console.error("Save awards transaction failed to begin, rolling back.", transactionBeginErr);
        transaction.rollback(rollbackErr => {
          if (rollbackErr) {
            console.error("Could not roll back!", rollbackErr);
          }
        });
      } else {

        try {
          const { segments } = row;
          delete row.segments;

          await transaction.request()
            .input('requestId', sql.Int, requestId)
            .input('engine', sql.VarChar, row.engine)
            .input('partner', sql.Bit, row.partner ? 1 : 0)
            .input('fromCity', sql.VarChar, row.fromCity)
            .input('toCity', sql.VarChar, row.toCity)
            .input('date', sql.VarChar, row.date)
            .input('cabin', sql.VarChar, row.cabin)
            .input('mixed', sql.Bit, row.mixed)
            .input('duration', sql.Int, row.duration)
            .input('stops', sql.Int, row.stops)
            .input('quantity', sql.Int, row.quantity)
            .input('mileage', sql.Int, row.mileage)
            .input('fees', sql.VarChar, row.fees)
            .input('fares', sql.VarChar, row.fares)
            .query('INSERT awards (requestId,engine,partner,fromCity,toCity,date,cabin,mixed,duration,stops,quantity,mileage,fees,fares) OUTPUT INSERTED.id ' + 
              'VALUES (@requestId, @engine, @partner, @fromCity, @toCity, @date, @cabin, @mixed, @duration, @stops, @quantity, @mileage, @fees, @fares)', async (err, result) => {
                if (err) {
                  console.error("Save award query failed", err);
                  transaction.rollback(rollbackErr => {
                    if (rollbackErr) {
                      console.error("Save award failed, and failed to roll back!", rollbackErr);
                    } else {
                      console.error("Save award failed, successfully rolled back.");
                    }
                  });
                } else {
                  const awardId = result.recordset[0].id;

                  // Now add each segment
                  // TODO: actually wait until transaction is committed to awards before doing this!!
                  if (segments) {
                    segments.forEach(async (segment, position) => {
                      await saveSegment(awardId, position, segment)
                    })
                  }
                } 
              });

          // for now, commit each award separately
          await transaction.commit(commitErr => {
            if (commitErr) {
              console.error("transaction commit failed, rolling back.", commitErr);
              transaction.rollback(rollbackErr => {
                if (rollbackErr) {
                  console.error("Could not roll back!", rollbackErr);
                }
              });
            } 
          });
        } catch (e) {
          console.error("unhandled exception while saving awards, rolling back.", e);
          transaction.rollback(rollbackErr => {
            if (rollbackErr) {
              console.error("unhandled exception, and could not roll back!", rollbackErr);
            }
          });
        }
      }
    });
  }
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
