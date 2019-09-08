const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;
const express = require('express')
const app = express()
const port = 3002

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
  });
} else {
  // Workers can share any TCP connection
  // In this case it is an HTTP server
  app.get('/', (req, res) => {
    for(let i=0;i<2e6;i++) {
  
    }
    res.send('Hello World!')
  })
  
  app.listen(port, () => console.log(`Example app listening on port ${port}!`))
}