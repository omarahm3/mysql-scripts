const mysql = require('async-mysql');

const init = (configs = {}) => {
  return mysql.createConnection ({
    host: configs.host || process.env.DB_HOST,
    user: configs.user || process.env.DB_USER,
    password: configs.password || process.env.DB_PASSWORD,
    database: configs.database || process.env.DB_NAME
  });
}

const connect = async (configs = {}) => {
  return mysql.connect({
    host: configs.host || process.env.DB_HOST,
    user: configs.user || process.env.DB_USER,
    password: configs.password || process.env.DB_PASSWORD,
    database: configs.database || process.env.DB_NAME
  })
}

module.exports = {
  init: init,
  connect: connect
}