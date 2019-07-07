const program = require('commander')
const fs = require('fs')
const prompt = require('syncprompt')
const timetable = require('timetable-fns')

const fp = require('../src')
const accounts = require('../shared/accounts')
const db = require('../shared/db')
const helpers = require('../shared/helpers')
const logger = require('../shared/logger')
const paths = require('../shared/paths')
const routes = require('../shared/routes')
const searchHelper = require('../shared/search')

program
  .option('-w, --website <airline>', 'IATA 2-letter code of the airline whose website to search')
  .option('-p, --partners', `Include partner awards (default: false)`)
  .option('-f, --from <city>', `IATA 3-letter code of the departure airport`)
  .option('-t, --to <city>', `IATA 3-letter code of the arrival airport`)
  .option('-o, --oneway', `Searches for one-way award inventory only (default: search both directions)`)
  .option('-c, --cabin <class>', `Cabin (${Object.keys(fp.cabins).join(', ')})`, (x) => (x in fp.cabins) ? x : false, undefined)
  .option('-s, --start <date>', `Starting date of the search range (YYYY-MM-DD)`, undefined)
  .option('-e, --end <date>', `Ending date of the search range (YYYY-MM-DD)`, undefined)
  .option('-q, --quantity <n>', `# of passengers traveling`, (x) => parseInt(x), 1)
  .option('-a, --account <n>', `Index of account to use`, (x) => parseInt(x), 0)
  .option('-h, --headless', `Run Chrome in headless mode`)
  .option('-p, --proxy <server>', `Provide a proxy to use with Chome (server:port:user:pass)`)
  .option('-d, --docker', `Enable flags to make allow execution in docker environment`)
  .option('-P, --no-parser', `Do not parse search results`)
  .option('-r, --reverse', `Run queries in reverse chronological order`)
  .option('--terminate <n>', `Terminate search if no results are found for n successive days`, (x) => parseInt(x), 0)
  .option('--force', 'Re-run queries, even if already in the database')
  .option('--debug [port]', 'Enable remote debugging port for headless Chrome (default: port 9222)', (x) => parseInt(x))
  .on('--help', () => {
    console.log('')
    console.log('  Supported Websites:')
    console.log('')
    fp.supported().forEach(id => console.log(`    ${id} - ${fp.new(id).config.name}`))
  })
  .parse(process.argv)

function fatal (engine, message, err) {
  if (typeof engine === 'string') {
    err = message
    message = engine
    engine = null
  }
  engine ? engine.error(message) : logger.error(message)
  if (err) {
    console.error(err)
  }
  process.exit(1)
}

function populateArguments (args) {
  // Default to one-day search if end date is not specified
  if (args.start && !args.end) {
    args.end = args.start
  }

  // Fill in missing arguments
  if (!args.website) {
    args.website = prompt('Airline website to search (2-letter code)? ')
  }
  if (!args.from) {
    args.from = prompt('Departure city (3-letter code)? ')
  }
  if (!args.to) {
    args.to = prompt('Arrival city (3-letter code)? ')
  }
  if (!args.cabin) {
    args.cabin = prompt(`Desired cabin class (${Object.keys(fp.cabins).join('/')})? `)
  }
  if (!args.start) {
    args.start = prompt('Start date of search range (YYYY-MM-DD)? ')
  }
  if (!args.end) {
    args.end = prompt('End date of search range (YYYY-MM-DD)? ')
  }
  args.partners = !!args.partners
  args.oneway = !!args.oneway
  args.headless = !!args.headless
  args.docker = !!args.docker
  args.parser = !!args.parser
  args.force = !!args.force
  args.debug = (args.debug === true) ? 9222 : args.debug
}

function validateArguments (args) {
  // Validate arguments
  if (!fp.supported(args.website || '')) {
    fatal(`Unsupported airline website to search: ${args.website}`)
  }
  if (!(args.cabin in fp.cabins)) {
    fatal(`Unrecognized cabin specified: ${args.cabin}`)
  }
  if (!timetable.valid(args.start)) {
    fatal(`Invalid start date: ${args.start}`)
  }
  if (!timetable.valid(args.end)) {
    fatal(`Invalid end date: ${args.end}`)
  }
  if (args.end < args.start) {
    fatal(`Invalid date range: ${args.start} - ${args.end}`)
  }
  if (args.quantity < 1) {
    fatal(`Invalid quantity: ${args.quantity}`)
  }
  if (args.account < 0) {
    fatal(`Invalid account index: ${args.account}`)
  }
  if (args.terminate < 0) {
    fatal(`Invalid termination setting: ${args.terminate}`)
  }

  // Instantiate engine, and do further validation
  const engine = fp.new(args.website)
  const { config } = engine

  // Calculate the valid range allowed by the engine
  const { minDays, maxDays } = config.validation
  const [a, b] = config.validDateRange()

  // Check if our search range is completely outside the valid range
  if (args.end < a || args.start > b) {
    fatal(engine, `Can only search within the range: ${a} - ${b}`)
  }

  // If only start or end are outside the valid range, we can adjust them
  if (args.start < a) {
    engine.warn(`Can only search from ${minDays} day(s) from today, adjusting start of search range to: ${a}`)
    args.start = a
  }
  if (args.end > b) {
    engine.warn(`Can only search up to ${maxDays} day(s) from today, adjusting end of search range to: ${b}`)
    args.end = b
  }

  // Parse proxy
  if (args.proxy) {
    const arr = args.proxy.split(':')
    if (arr.length === 0 || arr.length > 4) {
      fatal(`Unrecognized proxy format: ${args.proxy}`)
    }
    if (arr.length <= 2) {
      args.proxy = { server: arr.join(':') }
    } else {
      const [ user, pass ] = arr.splice(-2)
      args.proxy = { server: arr.join(':'), user, pass }
    }
  }
}

const main = async (args) => {

  await searchHelper.doSearch(args);

}

populateArguments(program)
validateArguments(program)
main(program)
