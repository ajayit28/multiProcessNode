const express = require('express')
const app = express()
const port = 3001

app.get('/', (req, res) => {
  for(let i=0;i<2e6;i++) {

  }
  res.send('Hello World!')
})

app.listen(port, () => console.log(`Example app listening on port ${port}!`))