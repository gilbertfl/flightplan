module.exports = {
  credentials: (process.env.MOUNTPATH || '.') + '/config/accounts.txt',
  oldCredentials: (process.env.MOUNTPATH || '.') + '/config/accounts.json',
  database: process.env.SQLSERVERURL || '*.database.windows.net',
  databaseName: process.env.SQLDBNAME || 'flightplan', 
  databaseUser: process.env.SQLUSER || '', 
  databasePassword: process.env.SQLPASS || '', 
  data: (process.env.MOUNTPATH || '.') + '/data'
}
