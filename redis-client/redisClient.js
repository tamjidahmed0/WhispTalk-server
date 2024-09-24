// redisClient.js
import {Redis} from 'ioredis'
import * as dotenv from 'dotenv' 
dotenv.config()


const options = {
  host: 'localhost',
  port: 16767,

}

//process.env.REDIS_URL

const client = new Redis();

// Handle Redis client errors
client.on('error', (err) => {
  console.error('Error connecting to Redis', err);
});

// Connect to Redis server
client.on('connect', () => { 
  console.log(`Redis connected to port ${options.port}`);
});

// Export the client to use it in other modules
export default client
