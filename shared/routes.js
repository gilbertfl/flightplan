const chalk = require('chalk');

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

async function find (dbPool, route) {
  const map = new Map()

  // Update map with award requests
  for (const row of await requests(dbPool, route)) {
    const { departDate, returnDate } = row
    let obj = getOrSet(map, key(row, departDate))
    obj.requests.push(row)
    if (returnDate) {
      obj = getOrSet(map, key(row, returnDate, true))
      obj.requests.push(row)
    }
  }

  // Now update with awards
  for (const row of await awards(dbPool, route)) {
    let obj = getOrSet(map, key(row, row.date))
    obj.awards.push(row)
  }

  return map
}


async function requests (dbPool, route) {
  // If no route defined, just select all records
  if (!route) {
    return await db.getAllRequests(dbPool);
  }

  const { engine, partners, cabin, quantity, fromCity, toCity, departDate, returnDate } = route

  // Select only the relevant segments
  if (returnDate) {
    return await db.getRequestsForRT(dbPool, route);
  } else {
    return await db.getRequestsForOW(dbPool, route);
  }
}

async function awards (dbPool, route) {
  // If no route defined, just select all records
  if (!route) {
    return await db.getAllAwards(dbPool);
  }

  // Format dates
  const { engine, cabin, quantity, fromCity, toCity, departDate, returnDate } = route
  const departStr = departDate || null
  const returnStr = returnDate || null

  // Select only the relevant segments
  if (returnDate) {
    // Round-Trip route
    return await db.getAwardsForRT(dbPool, route);
  } else {
    // One-Way route
    return await db.getAwardsForOW(dbPool, route);
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
