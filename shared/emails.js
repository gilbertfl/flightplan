var imaps = require('imap-simple');
const simpleParser = require('mailparser').simpleParser;
const _ = require('lodash');
 
var config = {
  imap: {
    user: process.env.IMAPUSER,
    password: process.env.IMAPPASS,
    host: process.env.IMAPHOST,
    tlsOptions: { servername: process.env.IMAPHOST }, // fixes DEPTH_ZERO_SELF_SIGNED_CERT error in windows because https://github.com/nodejs/node/issues/28167
    port: Number(process.env.IMAPPORT),
    tls: process.env.IMAPTLS && process.env.IMAPTLS.toLowerCase() == "true" ? true : false,
    authTimeout: 3000
  }
};

async function findVerificationCodeInEmail(fromEmail) {
  var connection = await imaps.connect(config);

  await connection.openBox('INBOX');

  //var searchCriteria = ['1:5'];
  var searchCriteria = ['UNSEEN', ['FROM', fromEmail]];
  var fetchOptions = {
    markSeen: true, 
    bodies: ['HEADER', 'TEXT', ''],
  };
  var messages = await connection.search(searchCriteria, fetchOptions)
  
  if (messages.length <= 0) {
    // nothing (at least, not yet!)
    console.log("No verification email found yet.");
    return "";
  }

  // TODO: account for case when we have multiple unread verification emails!
  var item = messages[0];

  var all = _.find(item.parts, { "which": "" })
  var id = item.attributes.uid;
  var idHeader = "Imap-Id: "+id+"\r\n";
  let mail = await simpleParser(idHeader+all.body);
  
  // access to the whole mail object
  console.log(`Found email with potential verification code. The email subject is: ${mail.subject}`);

  // TODO: in mail.html, look for "<strong>VERIFICATION CODE: 900528</strong>" and parse the code out!
  const regex = /<strong>VERIFICATION CODE: ([0-9][0-9][0-9][0-9][0-9][0-9])<\/strong>/g;
  const found = regex.exec(mail.html);

  // if the regex worked, the verification code is the 2nd item in the array
  if (found && found.length > 1) {
    const verificationCode = found[1];
    console.log(`Found verification code within email. The verification code is: ${verificationCode}`);

    // now delete email since we don't need it any more
    await connection.deleteMessage(id);

    return verificationCode;
  }

  // couldn't find verification code!
  console.error(`Couldn't find verification code in email with subject: ${mail.subject}`);
  return "";
}

module.exports = {
  findVerificationCodeInEmail
}
  
