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

// Engine-specific search strategies
const strategies = {
    cx: { roundtripOptimized: false },
    ke: { oneWaySupported: false },
    nh: { roundtripOptimized: false }
}

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
    const departCities = { fromCity: args.from, toCity: args.to }
    const returnCities = { fromCity: args.to, toCity: args.from }

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

    async function redundant (query) {
    const { departDate, returnDate } = query

    // Lookup associated routes from database
    const map = await routes.find(query)

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

async function doSearch(args) {
    // Create engine
    const engine = fp.new(args.website)
    let initialized = false

    try {
        // Create data path if necessary
        if (!fs.existsSync(paths.data)) {
        fs.mkdirSync(paths.data)
        }

        // Create database if necessary, and then open
        await db.migrate()
        await db.open()

        // Setup engine options
        const options = { headless, proxy, docker }
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
        console.log(`Searching ${days} days of award inventory: ${timetable.format(startDate)} - ${timetable.format(endDate)}`)
        for (const query of queries) {
        const { id, loginRequired } = engine

        // Check if the query's results are already stored
        if (!args.force && await redundant(query)) {
            skipped++
            continue
        }

        // Should we terminate?
        if (terminate && parse && query.departDate !== lastDate) {
            daysRemaining--
            lastDate = query.departDate
            if (daysRemaining < 0) {
            console.log(`Terminating search after no award inventory found for ${terminate} days.`)
            }
        }

        // Lazy load the search engine
        if (!initialized) {
            const credentials = loginRequired
            ? accounts.getCredentials(id, args.account) : null
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
            console.error(err)
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
            console.error(err)
            continue
            }
        }

        // Write request and awards (if parsed) to database
        const requestId = await helpers.saveRequest(results);
        if (awards) {
            if (awards.length > 0) {
            daysRemaining = terminate // Reset termination counter
            }
            const placeholders = helpers.createPlaceholders(results, { cabins: Object.values(fp.cabins) })
            await helpers.saveAwards(requestId, awards, placeholders)
        }
        }
        if (skipped > 0) {
        console.log(`Skipped ${skipped} queries.`)
        }
        logger.success('Search complete!')
    } catch (err) {
        fatal('A fatal error occurred!', err)
    } finally {
        await engine.close()
        db.close()
    }
}

module.exports = {
    doSearch
}