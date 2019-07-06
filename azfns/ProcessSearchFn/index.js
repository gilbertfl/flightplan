module.exports = async function(context, mySbMsg) {
    context.log('JavaScript ServiceBus queue trigger function processed message', mySbMsg);

    // TODO: do a nodejs HTTP Post after search is complete to post to the PushBullet API
};