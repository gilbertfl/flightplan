const Searcher = require('../../Searcher')
const { cabins } = require('../../consts')
const readline = require('readline');
const { errors } = Searcher

const rl = readline.createInterface({ input: process.stdin , output: process.stdout });

const getLine = (function () {
    const getLineGen = (async function* () {
        for await (const line of rl) {
            yield line;
        }
    })();
    return async () => ((await getLineGen.next()).value);
})();

module.exports = class extends Searcher {
  async isLoggedIn (page) {
    await page.waitFor(
      'button.header-login-btn, a.header-logout-btn', {visible: true, timeout: 30000})
    return !!(await page.$('a.header-logout-btn'))
  }

  async login (page, credentials) {
    const [ username, password ] = credentials
    if (!username || !password) {
      throw new errors.MissingCredentials()
    }

    // Enter username and password
    await this.enterText('#cust', username)
    await this.enterText('#pin', password)
    await page.waitFor(250)

    // Check remember box, and submit the form
    if (!await page.$('div.checkbox input:checked')) {
      await page.click('div.checkbox input')
      await page.waitFor(250)
    }
    await this.clickAndWait('button.btn-primary.form-login-submit')

    // aeroplans new website is SUPER slow, and navigates multiple times before login dance is done
    console.info("...waiting 4 seconds for aeroplans website to load before checking if we need to verify...")
    await page.waitFor(4000)

    // is it waiting for a 2-factor code? if so, give us time to check email/phone and enter it manually
    // TODO: if email, programmatically access email (via imap config, or gmail api, or whatever) and fill it in via code
    const verificationCodeInput = await page.$('input[name=verification-code]')

    // TODO: instead, ask for verification code in console somehow, and then use page.$type to fill it in on the web page

    // if there is an input field and button asking for a verification code...
    if (verificationCodeInput) {
    
      console.info("Please enter Aeroplan required 6-digit verification code (from email/sms).")
      let verificationCode = await getLine();

      if (verificationCode != '' && verificationCode.length == 6) {

        await this.enterText('input[name=verification-code]', verificationCode)

        //await verificationCodeSubmitButton.click({delay: 100})
        await this.clickAndWait('button.submit-button')
      } else {
        console.error("verification code " + verificationCode + " is invalid.")
        throw new errors.InvalidVerificationCodeError()
      }
    }
    
    // // Check for errors
    // const msgError = await this.textContent('div.form-msg-box.has-error span.form-msg')
    // if (msgError.includes('does not match our records')) {
    //   throw new errors.InvalidCredentials()
    // }
    // const msgError2 = await this.textContent('div.form-msg-box.error.form-main-msg span.form-msg')
    // if (msgError2.includes('your account has been blocked')) {
    //   throw new errors.BlockedAccount()
    // }

    console.info("login complete (NOTE: for now, no error checking).")
  }

  async search (page, query, results) {
    const { oneWay, fromCity, toCity, cabin, quantity } = query
    const departDate = query.departDateMoment()
    const returnDate = query.returnDateMoment()

    // Wait a few seconds for the form to auto-fill itself
    await page.waitFor(3000)

    // Get cabin values
    const cabinVals = [cabins.first, cabins.business].includes(cabin)
      ? ['Business/First', 'Business']
      : ['Eco/Prem', 'Economy']

    // Fill out the form
    if (oneWay) {
      await this.fillForm({
        tripTypeRoundTrip: 'One-way', // note: this is a bug in aeroplans DOM, they named both inputs the same...
        currentTripTab: 'oneway',
        city1FromOnewayCode: fromCity,
        city1ToOnewayCode: toCity,
        l1Oneway: departDate.format('MM/DD/YYYY'),
        l1OnewayDate: departDate.format('YYYY-MM-DD'),
        OnewayCabinTextfield: cabinVals[0],
        OnewayCabin: cabinVals[1],
        OnewayAdultsNb: quantity.toString(),
        OnewayChildrenNb: '0',
        OnewayTotalPassengerNb: quantity.toString(),
        OnewayFlexibleDatesHidden: '0'
      })
    } else {
      await this.fillForm({
        tripTypeRoundTrip: 'Round-Trip',
        currentTripTab: 'return',
        city1FromReturnCode: fromCity,
        city1ToReturnCode: toCity,
        l1Return: departDate.format('MM/DD/YYYY'),
        l1ReturnDate: departDate.format('YYYY-MM-DD'),
        r1Return: returnDate.format('MM/DD/YYYY'),
        r1ReturnDate: returnDate.format('YYYY-MM-DD'),
        ReturnCabinTextfield: cabinVals[0],
        ReturnCabin: cabinVals[1],
        ReturnAdultsNb: '1',
        ReturnChildrenNb: '0',
        ReturnTotalPassengerNb: '1',
        ReturnFlexibleDatesHidden: '0'
      })
    }

    // Submit the form, and capture the AJAX response
    await this.submitForm(oneWay
      ? 'travelFlightsOneWayTab'
      : 'travelFlightsRoundTripTab',
      { waitUntil: 'none' })

    // Wait for results to load
    await this.monitor('.waiting-spinner-inner')

    // Check for errors
    const msgError = await this.textContent('div.errorContainer')
    if (msgError.includes('itinerary is not eligible') || msgError.includes('itinerary cannot be booked')) {
      throw new errors.InvalidRoute()
    }

    // Wait up to 15 seconds to get the JSON from the browser itself
    let json = null
    await this.attemptWhile(
      async () => { return !json },
      async () => {
        await page.waitFor(1000)
        json = await page.evaluate(() => this.results ? this.results.results : null)
      },
      15,
      new Searcher.Error(`Timed out waiting for JSON results to be created`)
    )

    // Obtain the JSON from the browser itself, which will have calculated prices
    await results.saveJSON('results', json)
    await results.screenshot('results')
  }
}
