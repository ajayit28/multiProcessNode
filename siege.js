var siege = require('siege')
let port = process.argv[2];

siege()
  .on(port)
  .for(20000).times
  .get('/')
  .attack()