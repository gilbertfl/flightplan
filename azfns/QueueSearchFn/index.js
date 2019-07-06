module.exports = async function (context, req) {
    context.log('JavaScript HTTP trigger function processed a request.');

    if (req.query.name || (req.body && req.body.name)) {
        context.res = {
            // status: 200, /* Defaults to 200 */
            body: "Hello " + (req.query.name || req.body.name)
        };

        // TODO: fill up the SB queue msg with all the stuff from the HTTP post (req)
        // NOTE: looks like so far JS only supports setting the message itself (no metadata?)
        //   https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-service-bus#output---javascript-example
        context.bindings.outputSbQueue = (req.query.name || req.body.name);
    }
    else {
        context.res = {
            status: 400,
            body: "Please pass a name on the query string or in the request body"
        };
    }
};