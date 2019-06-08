const chalk = require('chalk');
const util = require('util');
const db = require('./db');
const paths = require('./paths');


function path (route) {
  const { engine, fromCity, toCity, departDate } = route
  const fields = [
    engine,
    fromCity,
    toCity,
    departDate,
    (new Date()).getTime()
  ]
  return `${paths.data}/${fields.join('-')}`
}

function key (row, date, reverse = false) {
  let { engine, cabin, fromCity, toCity } = row
  return [engine, cabin, reverse ? toCity : fromCity, reverse ? fromCity : toCity, date].join('|')
}

function getOrSet (map, key) {
  let ret = map.get(key)
  if (ret === undefined) {
    ret = { requests: [], awards: [] }
    map.set(key, ret)
  }
  return ret
}

async function find (route) {
  const map = new Map()

  const requestsResult = await requests(route);

  // Update map with award requests
  for (const row of requestsResult) {
    const { departDate, returnDate } = row
    let obj = getOrSet(map, key(row, departDate))
    obj.requests.push(row)
    if (returnDate) {
      obj = getOrSet(map, key(row, returnDate, true))
      obj.requests.push(row)
    }
  }

  const awardsResult = await awards(route);

  // Now update with awards
  for (const row of awardsResult) {
    let obj = getOrSet(map, key(row, row.date))
    obj.awards.push(row)
  }

  return map
}


async function requests (route) {

  console.log(route)

  // If no route defined, just select all records
  if (!route) {
    return db.getAllRequests();
  }

  const { engine, partners, cabin, quantity, fromCity, toCity, departDate, returnDate } = route

  // Select only the relevant segments
  if (returnDate) {
    return db.getRequestsForRT(route);
  } else {
    return db.getRequestsForOW(route);
  }
}

async function awards (route) {
  // If no route defined, just select all records
  if (!route) {
    var getAllAwardsFn = function (db, sql, requestCallback) {
      var request = new Request(sql, requestCallback);
      db.execSql(request);
    };
    getAllAwardsFn[util.promisify.custom] = (innerdb, innersql) => db.createPromiseFromRequest(innerdb, innersql, getAllAwardsFn);
    const getAllAwardsPromise = util.promisify(getAllAwardsFn);
    var allAwardsResult = await getAllAwardsPromise(database, 'SELECT * FROM awards');
    console.log("all awards result", allAwardsResult);
    return allAwardsResult.rows;
  }

  // Format dates
  const { engine, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null
  const returnStr = returnDate || null

  // Select only the relevant segments
  if (returnDate) {
    // Round-Trip route
    const sql = 'SELECT * FROM awards WHERE ' +
        'engine = @engine AND cabin = @cabin AND quantity <= @quantity AND (' +
        '(fromCity = @fromCity AND toCity = @toCity AND date = @departStr) OR ' +
        '(fromCity = @toCity AND toCity = @fromCity AND date = @returnStr))'
    var roundTripFn = function (db, sql, requestCallback) {
      var request = new Request(sql, requestCallback);  
      request.addParameter('engine', TYPES.VarChar, engine);
      request.addParameter('cabin', TYPES.VarChar, cabin);
      request.addParameter('quantity', TYPES.Int, quantity);
      request.addParameter('fromCity', TYPES.VarChar, fromCity);
      request.addParameter('toCity', TYPES.VarChar, toCity);
      request.addParameter('departStr', TYPES.VarChar, departStr);
      request.addParameter('returnStr', TYPES.VarChar, returnStr);
      db.execSql(request); 
    };
    roundTripFn[util.promisify.custom] = (innerdb, innersql) => db.createPromiseFromRequest(innerdb, innersql, roundTripFn);
    const roundTripPromise = util.promisify(roundTripFn);
    var result = await roundTripPromise(database, sql);
    return result.rows;
  } else {
    // One-Way route
    const sql = 'SELECT * FROM awards WHERE ' +
        'engine = @engine AND cabin = @cabin AND quantity <= @quantity AND ' +
        'fromCity = @fromCity AND toCity = @toCity AND date = @departStr'
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
    oneWayFn[util.promisify.custom] = (innerdb, innersql) => db.createPromiseFromRequest(innerdb, innersql, oneWayFn);
    const oneWayPromise = util.promisify(oneWayFn);
    var result = await oneWayPromise(database, sql);
    return result.rows;
  }
}

function print (route) {
  const { engine, fromCity, toCity, departDate, returnDate, quantity } = route

  // Passenger details
  const strPax = `${quantity} ${quantity > 1 ? 'Passengers' : 'Passenger'}`

  // Format dates if necessary
  const departStr = (departDate && typeof departDate !== 'string')
    ? departDate : departDate
  const returnStr = (returnDate && typeof returnDate !== 'string')
    ? returnDate : returnDate

  // Print departure and arrival routes
  const context = chalk.bold(`[${engine}]`)
  console.log(chalk.blue(`${context} DEPARTURE [${fromCity} -> ${toCity}] - ${departStr} (${strPax})`))
  if (returnDate) {
    console.log(chalk.blue(`${context} ARRIVAL   [${toCity} -> ${fromCity}] - ${returnStr}`))
  }
}

module.exports = {
  path,
  key,
  find,
  print
}
