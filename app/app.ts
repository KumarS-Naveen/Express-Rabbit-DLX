import express = require('express');
 import messageQ from './MessageQ';


// Create a new express application instance
const app: express.Application = express();
messageQ.init();

app.listen(3001, function () {
  console.log('Example app listening on port 3001!');
});


