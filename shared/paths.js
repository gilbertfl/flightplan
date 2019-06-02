module.exports = {
  credentials: (process.env.MOUNTPATH || '.') + '/config/accounts.txt',
  oldCredentials: (process.env.MOUNTPATH || '.') + '/config/accounts.json',
  database: (process.env.MOUNTPATH || '.') + '/db/database.sqlite3',
  data: (process.env.MOUNTPATH || '.') + '/data'
}
