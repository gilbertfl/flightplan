const sql = require('mssql')
const util = require('util')
const paths = require('./paths')
const utils = require('../src/utils')

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
      throw err;
    });
  }

  return _pool;
}

async function getRequestsWithoutAwards(engine, force) {
  // Select only those requests without corresponding entries in awards table
  let sql = force
    ? 'SELECT * FROM requests'
    : 'SELECT requests.* FROM requests LEFT JOIN awards ON requests.id = awards.requestId WHERE requestId IS NULL';
  if (engine) {
    sql += `${force ? ' WHERE' : ' AND'} requests.engine = @engine`;
    return await _pool.request().input('engine').query(sql);
  } else {
    return await _pool.request().query(sql);
  }
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

async function getSegments(awardId) {
  var result = await _pool.request()
    .input('awardId', sql.Int, awardId)
    .query('SELECT * FROM segments WHERE awardId = @awardId');
  return result.recordset;
}

async function cleanupRequest(requestId) {
  var result = await _pool.request()
    .input('requestId', sql.Int, requestId)
    .query('DELETE FROM requests WHERE id = @requestId');
  return result.recordset;
}

async function getRequest(requestId) {
  var result = await _pool.request()
    .input('requestId', sql.Int, requestId)
    .query('SELECT * FROM requests WHERE id = @requestId');
  return result.recordset;
}

async function cleanupAwards(awards) {
  const transaction = _pool.transaction();

  transaction.begin(async transactionBeginErr => {
    if (transactionBeginErr) {
      console.error("save segment transaction failed to begin.", transactionBeginErr);
    } else {
      try {
        for (const award of awards) {
          await transaction.request()
            .input('awardId', sql.Int, award.id)
            .query('DELETE FROM segments WHERE awardId = @awardId');
          await transaction.request()
            .input('awardId', sql.Int, award.id)
            .query('DELETE FROM awards WHERE id = @awardId');
        }
        
        transaction.commit(commitErr => {
          if (commitErr) {
            console.error("transaction commit failed, rolling back.", commitErr);
            transaction.rollback(rollbackErr => {
              if (rollbackErr) {
                console.error("Cleanup awards failed, and failed to roll back!", rollbackErr);
              } else {
                console.error("Cleanup awards failed, successfully rolled back.");
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

function doSaveSegment(transaction, awardId, position, row, resolve, reject) {
  transaction.begin(transactionBeginErr => {
    if (transactionBeginErr) {
      console.error("save segment transaction failed to begin.", transactionBeginErr);
    } else {
      try {
        transaction.request()
            .input('awardId', sql.Int, awardId)
            .input('position', sql.Int, position)
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
            .query('INSERT segments (awardId, position, airline, flight, aircraft, fromCity, toCity, date, departure, arrival, duration, nextConnection, cabin, stops, lagDays, bookingCode) OUTPUT INSERTED.id ' + 
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
doSaveSegment[util.promisify.custom] = (transaction, awardId, position, row) => {
  return new Promise((resolve, reject) => {
    doSaveSegment(transaction, awardId, position, row, resolve, reject);
  });
};
async function saveSegment(awardId, position, row) {
  var promisifiedSaveSegment = util.promisify(doSaveSegment);
  return await promisifiedSaveSegment(_pool.transaction(), awardId, position, row);
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
async function saveRequest(row) {
  var promisifiedSaveRequest = util.promisify(doSaveRequest);
  return await promisifiedSaveRequest(_pool.transaction(), row);
}


function doSaveAward(transaction, requestId, row, resolve, reject) {
    transaction.begin(transactionBeginErr => {
      if (transactionBeginErr) {
        console.error("Save awards transaction failed to begin, rolling back.", transactionBeginErr);
        transaction.rollback(rollbackErr => {
          if (rollbackErr) {
            console.error("Could not roll back!", rollbackErr);
          }
        });
        reject(transactionBeginErr);
      } else {
        try {
          const { segments } = row;
          delete row.segments;

          transaction.request()
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
            .input('mileage', sql.Float, row.mileage)
            .input('fees', sql.VarChar, row.fees)
            .input('fares', sql.VarChar, row.fares)
            .query('INSERT awards (requestId,engine,partner,fromCity,toCity,date,cabin,mixed,duration,stops,quantity,mileage,fees,fares) OUTPUT INSERTED.id ' + 
              'VALUES (@requestId, @engine, @partner, @fromCity, @toCity, @date, @cabin, @mixed, @duration, @stops, @quantity, @mileage, @fees, @fares)', (err, result) => {
                if (err) {
                  console.error("Save award query failed", err);
                  transaction.rollback(rollbackErr => {
                    if (rollbackErr) {
                      console.error("Save award failed, and failed to roll back!", rollbackErr);
                    } else {
                      console.error("Save award failed, successfully rolled back.");
                    }
                  });
                  reject(err);
                } else {
                  const awardId = result.recordset[0].id;

                  // for now, commit each award separately
                  transaction.commit(commitErr => {
                    if (commitErr) {
                      console.error("transaction commit failed, rolling back.", commitErr);
                      transaction.rollback(rollbackErr => {
                        if (rollbackErr) {
                          console.error("Could not roll back!", rollbackErr);
                        }
                      });
                      reject(commitErr);
                    } 
                  });

                  // Now add each segment
                  // TODO: actually wait until transaction is committed to awards before doing this!!
                  if (segments) {
                    // TODO: use better javascript skills to do this!!
                    var segmentsArray = [];
                    segments.forEach((segment, position) => {
                      segmentsArray.push({
                        segment: segment, 
                        awardId: awardId
                      });
                    })
                    resolve(segmentsArray);
                  } else {
                    resolve(null);
                  }
                } 
              });
        } catch (e) {
          console.error("unhandled exception while saving awards, rolling back.", e);
          transaction.rollback(rollbackErr => {
            if (rollbackErr) {
              console.error("unhandled exception, and could not roll back!", rollbackErr);
            }
          });
          reject(e);
        }
      }
    });
  
}
doSaveAward[util.promisify.custom] = (pool, requestId, row) => {
  return new Promise((resolve, reject) => {
    const transaction = pool.transaction();
    doSaveAward(transaction, requestId, row, resolve, reject);
  });
};
async function saveAwards(requestId, rows) {
  var promisifiedSaveAward = util.promisify(doSaveAward);
  for (const row of rows) {
    // if saving fails, promise is rejected and an exception *should* be thrown
    var segments = await promisifiedSaveAward(_pool, requestId, row);
    if (segments) {
      for (let i=0; i<segments.length; i++) {
        await saveSegment(segments[i].awardId, i, segments[i].segment);
      }
    }
  }
}

async function doSearch(fromCity, toCity, quantity, direction, startDate, endDate, cabin, limit) {

  // Validate dates
  if (!utils.validDate(startDate)) {
    throw new Error('Invalid start date:', startDate);
  }
  if (!utils.validDate(endDate)) {
    throw new Error('Invalid end date:', endDate);
  }
  if (endDate < startDate) {
    throw new Error(`Invalid date range for search: ${startDate} -> ${endDate}`);
  }

  var awardsRequest = _pool.request();
  var segmentsRequest = _pool.request();
  let query = ' WHERE ';

  // Add cities
  if (direction === 'oneway') {
    query += 'a.fromCity = @fromCity AND a.toCity = @toCity';
  } else if (direction === 'roundtrip') {
    query += '((a.fromCity = @fromCity AND a.toCity = @toCity) OR (a.toCity = @fromCity AND a.fromCity = @toCity))';
  } else {
    throw new Error('Unrecognized direction parameter:', direction);
  }
  awardsRequest = awardsRequest.input('fromCity', sql.VarChar, fromCity.toUpperCase());
  awardsRequest = awardsRequest.input('toCity', sql.VarChar, toCity.toUpperCase());
  segmentsRequest = segmentsRequest.input('fromCity', sql.VarChar, fromCity.toUpperCase());
  segmentsRequest = segmentsRequest.input('toCity', sql.VarChar, toCity.toUpperCase());

  // Add dates
  query += ' AND a.date BETWEEN @startDate AND @endDate';
  awardsRequest = awardsRequest.input('startDate', sql.VarChar, startDate);
  awardsRequest = awardsRequest.input('endDate', sql.VarChar, endDate);
  segmentsRequest = segmentsRequest.input('startDate', sql.VarChar, startDate);
  segmentsRequest = segmentsRequest.input('endDate', sql.VarChar, endDate);

  // Add quantity
  query += ' AND a.quantity >= @quantity';
  awardsRequest = awardsRequest.input('quantity', sql.Int, quantity);
  segmentsRequest = segmentsRequest.input('quantity', sql.Int, quantity);

  // Add cabins
  if (cabin) {
    const values = cabin.split(',');
    //query += ` AND cabin IN (${values.map(x => '?').join(',')})`;
    //values.forEach(x => params.push(x));

    // TODO: parameterize this 'IN' query!
    query += ` AND a.cabin IN (${values.map(x => `'${x}'`)})`;
  }

  // Add limit
  if (limit) {
    query += ' LIMIT @resultLimit';
    awardsRequest = awardsRequest.input('resultLimit', sql.Int, limit);
    segmentsRequest = segmentsRequest.input('resultLimit', sql.Int, limit);
  }

  // Run SQL query
  const awardQuery = 'SELECT a.* FROM awards as a ' + query;
  var awardsResult = await awardsRequest.query(awardQuery);

  const segmentsQuery = 'SELECT s.* FROM awards as a JOIN segments as s ON (a.id = s.awardId) ' + query; 
  var allSegmentsResult = await segmentsRequest.query(segmentsQuery)

  var toReturn = awardsResult.recordset;
  
  // TODO: assemble all of this in SQL instead of code!!
  for (var i=0; i<toReturn.length; i++) {
    const segments = allSegmentsResult.recordset.filter(s => s.awardId == toReturn[i].id);
    if (segments) {
      toReturn[i].segments = segments;
    } else {
      // for now just put null tuple
      toReturn[i].segments = [];
    }
  }

  return toReturn;
}

async function getAllRequestsForEngine(engine) {
  let sql = `SELECT * FROM requests`;
  if (engine) {
    sql += ' WHERE engine = @engine';
    return await _pool.request().input('engine', sql.VarChar, engine).query(sql);
  } else {
    return await _pool.request().query(sql);
  }
}

async function getAllAwardsForEngine(engine) {
  let sql = `SELECT * FROM awards`;
  if (engine) {
    sql += ' WHERE engine = @engine';
    return await _pool.request().input('engine', sql.VarChar, engine).query(sql);
  } else {
    return await _pool.request().query(sql);
  }
}

async function migrate() {
  // TODO (if desired): implement some kind of sqllite migration
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
  getRequestsWithoutAwards, 
  getAllRequestsForEngine, 
  getRequest, 
  getSegments, 
  getAllAwards, 
  getAllAwardsForEngine, 
  getAwardsForRT,
  getAwardsForOW, 
  open,
  saveAwards, 
  saveSegment, 
  saveRequest, 
  cleanupRequest,
  cleanupAwards, 
  doSearch, 
  close, 
  migrate
}
