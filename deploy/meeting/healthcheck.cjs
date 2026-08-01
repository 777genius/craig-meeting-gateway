'use strict';

const fs = require('node:fs');
const net = require('node:net');

function connect(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port: Number(port) });
    const timeout = setTimeout(() => socket.destroy(new Error(`timeout connecting to ${host}:${port}`)), 1500);
    socket.once('connect', () => {
      clearTimeout(timeout);
      socket.end();
      resolve();
    });
    socket.once('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

(async () => {
  fs.accessSync('/run/craig-config/startup.ready', fs.constants.R_OK);
  fs.accessSync('/run/craig-config/production.json', fs.constants.R_OK);
  await Promise.all([
    connect(process.env.POSTGRES_HOST || 'db', process.env.POSTGRES_PORT || 5432),
    connect(process.env.REDIS_HOST || 'redis', process.env.REDIS_PORT || 6379)
  ]);
})().catch((error) => {
  console.error(`Craig healthcheck failed: ${error.message}`);
  process.exitCode = 1;
});
