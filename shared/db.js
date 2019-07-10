const Database = require('better-sqlite3')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const utils = require('../src/utils')
const paths = require('./paths')
const prompts = require('../shared/prompts')

let _db = null

function open () {
  if (!_db) {
    _db = new Database(paths.database)
  }
  return _db
}

function detectOldVersion () {
  let db = null
  try {
    db = new Database(paths.database)
    return !!db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='awards_requests'`).get()
  } finally {
    if (db) {
      db.close()
    }
  }
}

function migrate () {
  if (fs.existsSync(paths.database)) {
    let migrationNeeded = false
    if (detectOldVersion()) {
      if (prompts.askYesNo(`
ERROR: An older version database was detected, that is incompatible with this version of Flightplan.

Would you like to convert it to the newer format? (WARNING: All search and award data will be deleted!)`)) {
        fs.unlinkSync(paths.database)
        rimraf.sync(paths.data)
        migrationNeeded = true
      }
    }
    if (!migrationNeeded) {
      return
    }
  }
  console.log('Creating database...')

  // Create database directory if missing
  const dir = path.dirname(paths.database)
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir)
  }

  // Create the database, and tables
  try {
    _db = open()
    createTable('requests', [
      'id INTEGER PRIMARY KEY ASC',
      'engine TEXT NOT NULL',
      'partners BOOLEAN NOT NULL',
      'fromCity TEXT NOT NULL',
      'toCity TEXT NOT NULL',
      'departDate TEXT NOT NULL',
      'returnDate TEXT',
      'cabin TEXT',
      'quantity INTEGER DEFAULT 0',
      'assets TEXT NOT NULL',
      'updatedAt TEXT DEFAULT CURRENT_TIMESTAMP'
    ])
    createTable('awards', [
      'id INTEGER PRIMARY KEY ASC',
      'requestId INTEGER',
      'engine TEXT NOT NULL',
      'partner BOOLEAN NOT NULL',
      'fromCity TEXT NOT NULL',
      'toCity TEXT NOT NULL',
      'date TEXT NOT NULL',
      'cabin TEXT NOT NULL',
      'mixed BOOLEAN NOT NULL',
      'duration INTEGER',
      'stops INTEGER DEFAULT 0',
      'quantity INTEGER DEFAULT 1',
      'mileage INTEGER',
      'fees TEXT',
      'fares TEXT',
      'updated_at TEXT DEFAULT CURRENT_TIMESTAMP'
    ])
    createTable('segments', [
      'id INTEGER PRIMARY KEY ASC',
      'awardId INTEGER',
      'position INTEGER NOT NULL',
      'airline TEXT NOT NULL',
      'flight TEXT NOT NULL',
      'aircraft TEXT',
      'fromCity TEXT NOT NULL',
      'toCity TEXT NOT NULL',
      'date TEXT NOT NULL',
      'departure TEXT NOT NULL',
      'arrival TEXT NOT NULL',
      'duration INTEGER',
      'nextConnection INTEGER',
      'cabin TEXT',
      'stops INTEGER DEFAULT 0',
      'lagDays INTEGER DEFAULT 0',
      'bookingCode TEXT',
      'updated_at TEXT DEFAULT CURRENT_TIMESTAMP'
    ])
  } catch (err) {
    throw new Error(`Database migration failed: ${err.message}`)
  }
}

function createTable (tableName, columns) {
  return _db.prepare(`CREATE TABLE ${tableName} (${columns.join(',')})`).run()
}

async function getRequestsWithoutAwards(engine, force) {
  const bind = []

  // Select only those requests without corresponding entries in awards table
  let sql = force
    ? 'SELECT * FROM requests'
    : 'SELECT requests.* FROM requests LEFT JOIN awards ON requests.id = awards.requestId WHERE requestId IS NULL'
  if (engine) {
    sql += `${force ? ' WHERE' : ' AND'} requests.engine = ?`
    bind.push(engine)
  }

  // Evaluate the SQL
  return _db.prepare(sql).all(...bind)
}

async function getRequestsForOW(route) {
  const sql = 'SELECT * FROM requests WHERE ' +
      'engine = ? AND partners = ? AND cabin = ? AND quantity = ? AND (' +
        '(fromCity = ? AND toCity = ? AND departDate = ?) OR ' +
        '(fromCity = ? AND toCity = ? AND returnDate = ?))'
  return _db.prepare(sql).all(
    engine, partners ? 1 : 0, cabin, quantity,
    fromCity, toCity, departStr,
    toCity, fromCity, departStr
  )
}

async function getRequestsForRT(route) {
  const sql = 'SELECT * FROM requests WHERE ' +
      'engine = ? AND partners = ? AND cabin = ? AND quantity = ? AND (' +
        '(fromCity = ? AND toCity = ? AND (departDate = ? OR returnDate = ?)) OR ' +
        '(fromCity = ? AND toCity = ? AND (departDate = ? OR returnDate = ?)))'
  return _db.prepare(sql).all(
    engine, partners ? 1 : 0, cabin, quantity,
    fromCity, toCity, departStr, returnStr,
    toCity, fromCity, returnStr, departStr
  )
}

async function getAwardsForRT(route) {
  const sql = 'SELECT * FROM awards WHERE ' +
      'engine = ? AND cabin = ? AND quantity <= ? AND (' +
        '(fromCity = ? AND toCity = ? AND date = ?) OR ' +
        '(fromCity = ? AND toCity = ? AND date = ?))'
  return _db.prepare(sql).all(
    engine, cabin, quantity,
    fromCity, toCity, departStr,
    toCity, fromCity, returnStr
  )
}

async function getAwardsForOW(route) {
  const sql = 'SELECT * FROM awards WHERE ' +
      'engine = ? AND cabin = ? AND quantity <= ? AND ' +
        'fromCity = ? AND toCity = ? AND date = ?'
  return _db.prepare(sql).all(
    engine, cabin, quantity,
    fromCity, toCity, departStr
  )
}

async function getAllRequests() {
  return _db.prepare('SELECT * FROM requests').all()
}

async function getAllAwards() {
  return _db.prepare('SELECT * FROM awards').all()
}

async function getSegments(awardId) { 
  // TODO: figure out who was supposed to call this!
}

async function cleanupRequest(requestId) {
  _db.prepare('DELETE FROM requests WHERE id = ?').run(request.id)
}

async function getRequest(requestId) {
  return _db.prepare('SELECT id FROM awards WHERE requestId = ?').all(requestId)
}

async function cleanupAwards(awards) {
  const stmtDelAward = db.db().prepare('DELETE FROM awards WHERE id = ?')
  const stmtDelSegments = db.db().prepare('DELETE FROM segments WHERE awardId = ?')

  db.begin()
  let success = false
  try {
    for (const award of awards) {
      stmtDelSegments.run(award.id)
      stmtDelAward.run(award.id)
    }
    success = true
  } finally {
    success ? db.commit() : db.rollback()
  }
}

async function saveSegment(awardId, position, row) {
  return db.insertRow('segments', row).lastInsertROWID
}

async function saveRequest(row) {
  return db.insertRow('requests', row).lastInsertROWID
}

async function saveAwards(requestId, rows) {
  // Wrap everything in a transaction
  let success = false
  db.begin()
  try {
    for (const row of rows) {
      const { segments } = row
      delete row.segments

      // Save the individual award and get it's ID
      row.requestId = requestId
      const awardId = db.insertRow('awards', row).lastInsertROWID
      ids.push(awardId)

      // Now add each segment
      if (segments) {
        segments.forEach((segment, position) => {
          saveSegment(awardId, position, segment)
        })
      }
    }
    success = true
  } finally {
    success ? db.commit() : db.rollback()
  }
  return success ? ids : null
}

async function doSearch(fromCity, toCity, quantity, direction, startDate, endDate, cabin, limit) {
}

async function getAllRequestsForEngine(engine) {
}

async function getAllAwardsForEngine(engine) {
}



function close () {
  if (_db) {
    _db.close()
    _db = null
  }
}

function begin () {
  _db.prepare('BEGIN').run()
}

function commit () {
  _db.prepare('COMMIT').run()
}

function rollback () {
  _db.prepare('ROLLBACK').run()
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
  saveAwards, 
  saveSegment, 
  saveRequest, 
  cleanupRequest,
  cleanupAwards, 
  doSearch, 
  close, 
  migrate
}
