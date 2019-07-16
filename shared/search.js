const fs = require('fs')
const timetable = require('timetable-fns')

const fp = require('../src')
const accounts = require('../shared/accounts')
const db = require('../shared/db')
const helpers = require('../shared/helpers')
const logger = require('../shared/logger')
const paths = require('../shared/paths')
const routes = require('../shared/routes')

// Engine-specific search strategies
const strategies = {
    cx: { roundtripOptimized: false },
    ke: { oneWaySupported: false },
    nh: { roundtripOptimized: false }
}

function generateQueries (args, engine, days) {
    const { start: startDate, end: endDate } = args
    const queries = []

    // Get search strategy based on engine
    const {
        roundtripOptimized = true,
        oneWaySupported = true,
        tripMinDays = 3
    } = strategies[engine.id.toLowerCase()] || {}
    const gap = (args.oneway || !roundtripOptimized) ? 0 : Math.min(tripMinDays, days)
    const validEnd = engine.config.validDateRange()[1]

    // Compute cities coming from, and to
    const departCities = { fromCity: args.from.toUpperCase(), toCity: args.to.toUpperCase() }
    const returnCities = { fromCity: args.to.toUpperCase(), toCity: args.from.toUpperCase() }

    // Compute the one-way segments coming back at beginning of search range
    for (let i = 0; i < gap; i++) {
        const date = timetable.plus(startDate, i)
        if (oneWaySupported) {
        queries.push({
            ...returnCities,
            departDate: date,
            returnDate: null
        })
        } else if (timetable.plus(date, tripMinDays) <= validEnd) {
        queries.push({
            ...returnCities,
            departDate: date,
            returnDate: timetable.plus(date, tripMinDays)
        })
        } else {
        queries.push({
            ...departCities,
            departDate: timetable.minus(date, tripMinDays),
            returnDate: date
        })
        }
    }

    // Compute segments in middle of search range
    for (let i = 0; i < days - gap; i++) {
        const date = timetable.plus(startDate, i)
        if (roundtripOptimized) {
        queries.push({
            ...departCities,
            departDate: date,
            returnDate: args.oneway ? null : timetable.plus(date, gap)
        })
        } else {
        queries.push({...departCities, departDate: date})
        if (!args.oneway) {
            queries.push({...returnCities, departDate: date})
        }
        }
    }

    // Compute the one-way segments going out at end of search range
    for (let i = gap - 1; i >= 0; i--) {
        const date = timetable.minus(endDate, i)
        if (oneWaySupported) {
        queries.push({
            ...departCities,
            departDate: date,
            returnDate: null
        })
        } else if (timetable.plus(date, tripMinDays) <= validEnd) {
        queries.push({
            ...departCities,
            departDate: date,
            returnDate: timetable.plus(date, tripMinDays)
        })
        } else {
        queries.push({
            ...returnCities,
            departDate: timetable.minus(date, tripMinDays),
            returnDate: date
        })
        }
    }

    // Fill in info that's universal for each query
    queries.forEach(q => {
        q.engine = engine.id
        q.partners = args.partners
        q.cabin = args.cabin
        q.quantity = args.quantity
        const routePath = routes.path(q)
        q.json = { path: routePath + '.json', gzip: true }
        q.html = { path: routePath + '.html', gzip: true }
        q.screenshot = { path: routePath + '.jpg', enabled: true }
    })

    return args.reverse ? queries.reverse() : queries
}

async function redundant (dbPool, query) {
    const { departDate, returnDate } = query

    // Lookup associated routes from database
    const map = await routes.find(dbPool, query)

    // Get departures
    const departures = map.get(routes.key(query, departDate))
    const departRedundant = redundantSegment(departures, query)
    if (!departRedundant) {
        return false
    }

    // Check returns
    if (returnDate) {
        const returns = map.get(routes.key(query, returnDate, true))
        const returnRedundant = redundantSegment(returns, query)
        if (!returnRedundant) {
        return false
        }
    }

    return true
}

function redundantSegment (routeMap, query) {
    const { quantity } = query
    if (routeMap) {
        if (routeMap.requests.find(x => x.quantity === quantity)) {
            return true // We've already run a request for this segment
        }
        if (routeMap.awards.find(x => x.segments && x.fares === '' && x.quantity <= quantity)) {
            return true // We already know this segment has no availability for an equal or lesser quantity
        }
    }
    return false
}

async function searchWebsiteForAwards(args, handleExceptions = true, customLogger = null) {

    const {
        start: startDate,
        end: endDate,
        headless,
        proxy,
        docker,
        parser: parse,
        terminate,
        debug: debugPort, 
        remotechrome, 
        credentials: credentialsToOverride
      } = args

    // Create engine
    const engine = fp.new(args.website)
    let initialized = false

    const dbPool = await db.createPool()

    try {
        // Create data path if necessary
        if (!fs.existsSync(paths.data)) {
            fs.mkdirSync(paths.data)
        }

        // Setup engine options
        const options = { headless, proxy, docker, remotechrome }
        if (debugPort) {
            options.args = [ `--remote-debugging-port=${debugPort}` ]
        }

        // Generate queries
        const days = timetable.diff(startDate, endDate) + 1
        const queries = generateQueries(args, engine, days)

        // Execute queries
        let skipped = 0
        let daysRemaining = terminate
        let lastDate = null
        
        if (customLogger) { customLogger(`Searching ${days} days of award inventory: ${timetable.format(startDate)} - ${timetable.format(endDate)}`); }
        else { console.log(`Searching ${days} days of award inventory: ${timetable.format(startDate)} - ${timetable.format(endDate)}`); }

        for (const query of queries) {
            const { id, loginRequired } = engine

            // Check if the query's results are already stored
            if (!args.force && await redundant(dbPool, query)) {
                skipped++
                continue
            }

            // Should we terminate?
            if (terminate && parse && query.departDate !== lastDate) {
                daysRemaining--
                lastDate = query.departDate
                if (daysRemaining < 0) {
                    if (customLogger) { customLogger(`Terminating search after no award inventory found for ${terminate} days.`); }
                    else { console.log(`Terminating search after no award inventory found for ${terminate} days.`); }
                }
            }

            // Lazy load the search engine
            if (!initialized) {
                const credentials = loginRequired
                    ? accounts.getCredentials(id, args.account, credentialsToOverride) : null
                await engine.initialize({ ...options, credentials })
                initialized = true
            }

            // Print route(s) being searched
            routes.print(query)

            // Run the search query, then check for searcher errors
            let results
            try {
                results = await engine.search(query)
                if (!results.ok) {
                    continue
                }
            } catch (err) {
                engine.error('Unexpected error occurred while searching!')

                if (customLogger) { customLogger(err); }
                else { console.error(err); }
                
                continue
            }

            // Parse awards, then check for parser errors
            let awards
            if (parse) {
                try {
                awards = results.awards
                if (!results.ok) {
                    engine.error(`Could not parse awards: ${results.error}`)
                    continue
                }
                engine.success(`Found: ${awards.length} awards, ${results.flights.length} flights`)
                } catch (err) {
                    engine.error('Unexpected error occurred while parsing!')
                    
                    if (customLogger) { customLogger(err); }
                    else { console.error(err); }

                    continue
                }
            }

            // Write request and awards (if parsed) to database
            const requestId = await helpers.saveRequest(dbPool, results);
            if (awards) {
                if (awards.length > 0) {
                    daysRemaining = terminate // Reset termination counter
                }
                const placeholders = helpers.createPlaceholders(results, { cabins: Object.values(fp.cabins) })
                await helpers.saveAwards(dbPool, requestId, awards, placeholders)
            }
        }
        if (skipped > 0) {
            if (customLogger) { customLogger(`Skipped ${skipped} queries.`); }
            else { console.log(`Skipped ${skipped} queries.`); }
        }

        if (customLogger) { customLogger('Search complete!'); }
        else { logger.success('Search complete!'); }

        return true
    } catch (err) {
        if (engine) {
            engine.error('A fatal error occurred!')
        } else {
            if (customLogger) { customLogger('A fatal error occurred!'); }
            else { logger.error('A fatal error occurred!'); }
        }
        if (err) {
            if (customLogger) { customLogger(err); }
            else { console.error(err); }
        }
        if (!handleExceptions) {
            throw err
        }
        return false
    } finally {
        await engine.close()
        dbPool.close()
    }
}

module.exports = {
    searchWebsiteForAwards
}