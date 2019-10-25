require('dotenv').config()

const mysql   = require('mysql');
const forEach = require('async-foreach').forEach;

const CONFIG      = {
  timeout: process.env.QUERY_TIMEOUT,
  breakOnError: false,
  tableName: 'report_chats',
  columnName: 'account_id'
}

const db = mysql.createConnection ({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

db.connect((err) => {
  if (err) {
      throw err;
  }
  console.log('--------------------------');
  console.log('##SCRIPT_LOG::: Connected to database');
  console.log('--------------------------');  


  const selectAccountIds = `SELECT DISTINCT ${CONFIG.columnName} FROM ${CONFIG.tableName}`;

  console.log(`##QUERY::: Executing Query: "${selectAccountIds}"`)

  db.query({sql: selectAccountIds, timeout: CONFIG.timeout}, (err, rows) => {
    if (err) {
      return handleError(err, selectAccountIds);
    }

    console.log('##QUERY::: Got account ids, looping...');

    forEach(rows, function(row) {
      var done = this.async();

      console.log('##QUERY::: ACCOUNT ID', row.account_id);
      console.log('==========================================')


      const tableName   = `${CONFIG.tableName}_${row.account_id}`;
      const createTable = `CREATE TABLE IF NOT EXISTS ${tableName} SELECT * FROM ${CONFIG.tableName} WHERE ${CONFIG.columnName} = ${row.account_id}`;

      console.log(`##QUERY::: Creating table ${tableName}`);

      db.query({sql: createTable, timeout: CONFIG.timeout}, (err) => {
        if (err) {
          return handleError(err, createTable);
        }

        console.log(`##QUERY::: table "${tableName}" created.`);
        done();
      })

      
      console.log('==========================================')
    }, allDone);

  })
});


function handleError(err, query) {
  console.log(`##SCRIPT_ERROR::: Error while running query "${query}"`);
  console.log('##SCRIPT_ERROR::: Error details', err);
  if (CONFIG.breakOnError) {
    process.exit()
  }
}

function allDone(notAborted, arr) {
  console.log('##SCRIPT_LOG::: All tables are done.')
  process.exit()
}