const express = require('express')

const fp = require('../src')
const db = require('../shared/db')
const logger = require('../shared/logger')

const app = express()
const port = process.env.PORT || 5000

// manually turn on CORS for all origins (TODO: use cors npm package instead)
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

app.get('/api/config', async (req, res, next) => {
  try {
    // Insert each website engine
    const engines = fp.supported().map((id) => {
      const config = fp.new(id).config.toJSON()
      const { name, website, fares } = config
      return { id, name, website, fares }
    })

    // Get list of all aircraft and airlines
    const aircraft = fp.aircraft
    const airlines = fp.airlines

    // Specify the available cabin options
    const cabins = [
      { value: fp.cabins.first, label: 'First' },
      { value: fp.cabins.business, label: 'Business' },
      { value: fp.cabins.premium, label: 'Prem. Economy' },
      { value: fp.cabins.economy, label: 'Economy' }
    ]

    res.send({engines, aircraft, airlines, cabins})
  } catch (err) {
    next(err)
  }
})

app.get('/api/search', async (req, res, next) => {
  try {
    const {
      fromCity = '',
      toCity = '',
      quantity = '1',
      direction = 'oneway',
      startDate = '',
      endDate = '',
      cabin,
      limit, 
      remotechrome = '', 
      credentials = ''
    } = req.query

    console.time('search')

    let awards = await db.doSearch(this.dbPool, fromCity, toCity, quantity, direction, startDate, endDate, cabin, limit, remotechrome, credentials)

    console.timeEnd('search')

    res.send(awards)
  } catch (err) {
    next(err)
  }
})

const main = async () => {
  try {
    this.dbPool = await db.createPool();

    // Launch Express server
    console.log(`Running web server on port: ${port}`)
    await app.listen(port)
    console.log('Success!')
  } catch (err) {
    logger.error(err)
  }
}

main()
