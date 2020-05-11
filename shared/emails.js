var imaps = require('imap-simple');
const simpleParser = require('mailparser').simpleParser;
const _ = require('lodash');
 
var config = {
  imap: {
    user: process.env.IMAPUSER,
    password: process.env.IMAPPASS,
    host: process.env.IMAPHOST,
    port: Number(process.env.IMAPPORT),
    tls: process.env.IMAPTLS.toLowerCase() == "true" ? true : false,
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
  console.log(mail.subject);
  console.log(mail.html);

  // TODO: in mail.html, look for "<strong>VERIFICATION CODE: 900528</strong>" and parse the code out!
  const regex = /<strong>VERIFICATION CODE: ([0-9][0-9][0-9][0-9][0-9][0-9])<\/strong>/g;
  const found = regex.exec(mail.html);

  // if the regex worked, the verification code is the 2nd item in the array
  const verificationCode = found[1];

  return verificationCode;
}

module.exports = {
  findVerificationCodeInEmail
}
  