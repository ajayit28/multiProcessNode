const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['10.0.0.22', '10.0.0.23', '10.0.0.24', '10.0.0.25', '10.0.0.26', '10.0.0.27'], localDataCenter: 'datacenter1', keyspace: 'orbiwise_dass' });



let queryExecute = (query) => {
  return new Promise((resolve, reject) => {
    client.execute(query, (err, results) => {
      if (err) {
        reject(err)
      } else {
        resolve(results)
      }
    })
  })
}

let batchExecute = (query) => {
  return new Promise((resolve, reject) => {
    client.batch(query, { prepare: true }, (err, results) => {
      if (err) {
        reject(err)
      } else {
        resolve(results)
      }
    })
  })
}

function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


function hex(value, numdig) {
  let res = value.toString(16);
  if (numdig != null) {
    res = "00000000000000" + res;
    res = res.substring(res.length - numdig);
  }
  return res;
}

var cluster = require("cluster");
if (cluster.isMaster) {
  // Forking Worker1 and Worker2
  var worker1 = cluster.fork({ WorkerName: "worker1" });
  var worker2 = cluster.fork({ WorkerName: "worker2" });
  var worker3 = cluster.fork({ WorkerName: "worker3" });
  var worker4 = cluster.fork({ WorkerName: "worker4" });
  var worker5 = cluster.fork({ WorkerName: "worker5" });
  var worker6 = cluster.fork({ WorkerName: "worker6" });
  var worker7 = cluster.fork({ WorkerName: "worker7" });
  var worker8 = cluster.fork({ WorkerName: "worker8" });
  var worker9 = cluster.fork({ WorkerName: "worker9" });
  var worker10 = cluster.fork({ WorkerName: "worker10" });
  var worker11 = cluster.fork({ WorkerName: "worker11" });
  var worker12 = cluster.fork({ WorkerName: "worker12" });
  var worker13 = cluster.fork({ WorkerName: "worker13" });
  var worker14 = cluster.fork({ WorkerName: "worker14" });
  var worker15 = cluster.fork({ WorkerName: "worker15" });
  var worker16 = cluster.fork({ WorkerName: "worker16" });

  // Respawn if one of both exits
  cluster.on("exit", function (worker, code, signal) {
    if (worker == worker1) worker1 = cluster.fork({ WorkerName: "worker1" });
    if (worker == worker2) worker2 = cluster.fork({ WorkerName: "worker2" });
    if (worker == worker3) worker3 = cluster.fork({ WorkerName: "worker3" });
    if (worker == worker4) worker4 = cluster.fork({ WorkerName: "worker4" });
    if (worker == worker5) worker5 = cluster.fork({ WorkerName: "worker5" });
    if (worker == worker6) worker6 = cluster.fork({ WorkerName: "worker6" });
    if (worker == worker7) worker7 = cluster.fork({ WorkerName: "worker7" });
    if (worker == worker8) worker8 = cluster.fork({ WorkerName: "worker8" });
    if (worker == worker9) worker9 = cluster.fork({ WorkerName: "worker9" });
    if (worker == worker10) worker10 = cluster.fork({ WorkerName: "worker10" });
    if (worker == worker11) worker11 = cluster.fork({ WorkerName: "worker11" });
    if (worker == worker12) worker12 = cluster.fork({ WorkerName: "worker12" });
    if (worker == worker13) worker13 = cluster.fork({ WorkerName: "worker13" });
    if (worker == worker14) worker14 = cluster.fork({ WorkerName: "worker14" });
    if (worker == worker15) worker15 = cluster.fork({ WorkerName: "worker15" });
    if (worker == worker16) worker16 = cluster.fork({ WorkerName: "worker16" });
  });
} else {


  if (process.env.WorkerName == "worker1") {
    // Code of Worker1
    insertData(0)
  }

  if (process.env.WorkerName == "worker2") {
    // Code of Worker2
    insertData(1)
  }

  if (process.env.WorkerName == "worker3") {
    // Code of Worker2
    insertData(2)
  }

  if (process.env.WorkerName == "worker4") {
    // Code of Worker2
    insertData(3)
  }

  if (process.env.WorkerName == "worker5") {
    // Code of Worker2
    insertData(4)
  }

  if (process.env.WorkerName == "worker6") {
    // Code of Worker2
    insertData(5)
  }

  if (process.env.WorkerName == "worker7") {
    // Code of Worker2
    insertData(6)
  }

  if (process.env.WorkerName == "worker8") {
    // Code of Worker2
    insertData(7)
  }

  if (process.env.WorkerName == "worker9") {
    // Code of Worker2
    insertData(8)
  }

  if (process.env.WorkerName == "worker10") {
    // Code of Worker2
    insertData(9)
  }

  if (process.env.WorkerName == "worker11") {
    // Code of Worker2
    insertData(10)
  }

  if (process.env.WorkerName == "worker12") {
    // Code of Worker2
    insertData(11)
  }

  if (process.env.WorkerName == "worker13") {
    // Code of Worker2
    insertData(12)
  }

  if (process.env.WorkerName == "worker14") {
    // Code of Worker2
    insertData(13)
  }

  if (process.env.WorkerName == "worker15") {
    // Code of Worker2
    insertData(14)
  }

  if (process.env.WorkerName == "worker15") {
    // Code of Worker2
    insertData(15)
  }
}


async function insertData(processid) {
  let count = 0;
  let numUsers = 15;
  let numDevicesPerUser = 1450;
  let deveuiPrefix = "76"
  for (let grp = 1; grp <= numUsers; grp++) {


    for (let gIdx = 121; gIdx < numDevicesPerUser; gIdx++) {

      let deveui = `${deveuiPrefix}${hex(processid, 2)}${hex(grp, 4)}00${hex(gIdx, 6)}`
      let selQuery = `SELECT * FROM ul_payloads where timestamp>1576281600000 and timestamp<1576382400000 and deveui='${deveui}' allow filtering;`

      let data = await queryExecute(selQuery)
      for (let row of data.rows) {
        for (let i = 1; i < 180; i++) {

          for (let j = 0; j < 2; j++) {
            // date - i (days)
            let ttl = 86400 * (180 - i)
            let currentDate = new Date(row.timestamp)
            currentDate.setDate(currentDate.getDate() - i);
            currentDate.setMinutes(currentDate.getMinutes() - (j * 20))
            let updatedDate = new Date(currentDate).toISOString()
            count++
            console.log(count)
            console.log(gIdx, process.pid, row.deveui)
            if (count % 5000 == 0) {
              console.log("Count: " + count)
              await timeout(15000)
            }

            // Preparing the insert query for each record day wise
            let insertQuery = `INSERT INTO ul_payloads (deveui,timestamp,altitude,confirmed,cr_used,device_redundancy,fcnt,freq,gateway_json,latitude,loc_accuracy,locerr_reason,longitude,mac_msg,no_push,payload_data,port,push_status,rssi,session_id,sf_used,snr,time_on_air_ms) VALUES ('${row.deveui}','${updatedDate}',${row.altitude},${row.confirmed},${row.cr_used},${row.device_redundancy},${row.fcnt},${row.freq},'${row.gateway_json}',${row.latitude},${row.loc_accuracy},'${row.locerr_reason}',${row.longitude},'${row.mac_msg}',${row.no_push},'${row.payload_data}',${row.port},'${row.push_status}',${row.rssi},'${row.session_id}',${row.sf_used},${row.snr},${row.time_on_air_ms}) using TTL ${ttl}`;

            try {
              await queryExecute(insertQuery);
            } catch(e) {
              console.log(JSON.stringify(e))
            }
            // if(count%1000 == 0) {
            //   try {
            //     await batchExecute(batchQuery)
            //   } catch (e) {
            //     console.log(JSON.stringify(e))
            //   }
            // } else {
            //   batchQuery.push(insertQuery);
            // }

           
          }

        }
      }


    }
  }




  // For number of days


}