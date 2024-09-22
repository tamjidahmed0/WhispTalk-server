import cluster from 'cluster';
import os from 'os';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
import express  from "express"
import { setupMaster, setupWorker } from '@socket.io/sticky';
import http from 'http'

const numCPUs = os.cpus().length;
const app = express()

if (cluster.isPrimary) {
  console.log(`Master ${process.pid} is running`);

  const httpServer = http.createServer();
  setupMaster(httpServer, {
    loadBalancingMethod: "least-connection", // either "random", "round-robin" or "least-connection"
  });
  httpServer.listen(1000);

  console.log('cpu number', numCPUs)

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork(); // Restart the worker
  });
} else {
  // Workers can share any TCP connection
  // In this case, it is an HTTP server
//   const __filename = fileURLToPath(import.meta.url);
//   const __dirname = dirname(__filename);

//   import(`${__dirname}/index.js`).then((module) => {
//     console.log(`Worker ${process.pid} started`);
//   });




const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const serverPath = new URL(`file://${path.join(__dirname, 'index.js')}`).href;

import(serverPath).then((module) => {
  console.log(`Worker ${process.pid} started`);
}).catch((error) => {
  console.error('Failed to load server module:', error);
});



}
