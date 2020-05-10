const fs = require('fs')

const db = require('./db')
const Query = require('../src/Query')
const Results = require('../src/Results')

function createPlaceholders (results, options = {}) {
  const { engine, query } = results
  const { fromCity, toCity, departDate, returnDate, quantity } = query
  const rows = []

  // Helper function to add a placeholder
  const fn = (fromCity, toCity, date, cabin) => {
    if (date) {
      rows.push({
        engine,
        fromCity,
        toCity,
        date,
        cabin,
        quantity,
        partner: false,
        mixed: false,
        stops: 0,
        fares: ''
      })
    }
  }

  // Add award placeholders, so we know what routes were searched
  const { cabins = [query.cabin] } = options
  for (const cabin of cabins) {
    fn(fromCity, toCity, departDate, cabin)
    fn(toCity, fromCity, returnDate, cabin)
  }
  return rows
}

function assetsForRequest (request) {
  const {
    html = [],
    json = [],
    screenshot = []
  } = JSON.parse(request.assets)
  return [...html, ...json, ...screenshot].map(x => x.path)
}

async function cleanupRequest (dbPool, request) {
  // Delete assets from disk
  for (const filename of assetsForRequest(request)) {
    if (fs.existsSync(filename)) {
      fs.unlinkSync(filename)
    }
  }

  // Remove from the database
  await db.cleanupRequest(dbPool, request.id);
}

async function cleanupAwards (dbPool, awards) {
  await db.cleanupAwards(dbPool, awards);
}

function loadRequest (row) {
  // Create Results from row
  return Results.parse({
    engine: row.engine,
    query: new Query({
      partners: row.partners,
      fromCity: row.fromCity,
      toCity: row.toCity,
      departDate: row.departDate,
      returnDate: row.returnDate,
      cabin: row.cabin,
      quantity: row.quantity
    }),
    ...JSON.parse(row.assets)
  })
}

async function saveRequest (dbPool, results) {
  // Get assets (only needs to have paths)
  const { assets } = results.trimContents()

  // Build the row data
  const { query } = results
  const row = {
    engine: results.engine,
    partners: query.partners,
    fromCity: query.fromCity,
    toCity: query.toCity,
    departDate: query.departDate,
    returnDate: query.returnDate,
    cabin: query.cabin,
    quantity: query.quantity,
    assets: JSON.stringify(assets)
  }

  // Insert the row
  return await db.saveRequest(dbPool, row);
}

async function saveAwards (dbPool, requestId, awards, placeholders) {
  

  // Transform objects to rows
  const rows = [ ...placeholders ]
  for (const award of awards) {
    rows.push({
      engine: award.engine,
      partner: award.partner,
      fromCity: award.flight.fromCity,
      toCity: award.flight.toCity,
      date: award.flight.date,
      cabin: award.fare.cabin,
      mixed: award.mixedCabin,
      duration: award.flight.duration,
      stops: award.flight.stops,
      quantity: award.quantity,
      mileage: award.mileageCost,
      fees: award.fees,
      fares: `${award.fare.code}${award.waitlisted ? '@' : '+'}`,
      segments: award.flight.segments
    })
  }

  try {
    await db.saveAwards(dbPool, requestId, rows);
  } catch (e) {
    console.error("entire saveAwards called threw exception", e);
    
    // we don't rethrow so that the search will keep on going (this 1 day just won't get saved)
  }
}

async function saveSegment (dbPool, awardId, position, segment) {
  // Build the row data
  const row = {
    airline: segment.airline,
    flight: segment.flight,
    aircraft: segment.aircraft,
    fromCity: segment.fromCity,
    toCity: segment.toCity,
    date: segment.date,
    departure: segment.departure,
    arrival: segment.arrival,
    duration: segment.duration,
    nextConnection: segment.nextConnection,
    cabin: segment.cabin,
    stops: segment.stops,
    lagDays: segment.lagDays
  }
  row.awardId = awardId
  row.position = position

  // Save the individual award and get it's ID
  return await db.saveSegment(dbPool, row);
}

module.exports = {
  createPlaceholders,
  assetsForRequest,
  cleanupRequest,
  cleanupAwards,
  loadRequest,
  saveRequest,
  saveAwards,
  saveSegment
}
