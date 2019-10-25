require('dotenv').config();

const mysql           = require('mysql');
const forEach         = require('async-foreach').forEach;

const CONFIG          = {
  timeout: process.env.QUERY_TIMEOUT,
  ignoredTables: [
    'report_chats'
  ],
  breakOnError: false
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

  const showTables = "SHOW TABLES";

  console.log(`##QUERY::: Executing Query: "${showTables}"`)

  db.query({sql: showTables, timeout: CONFIG.timeout}, (err, rows) => {
    if (err) {
      return handleError(err, selectAccountIds);
    }

    console.log(`##QUERY::: Got #${rows.length} tables, looping...`);

    forEach(rows, function(row) {
      const tableName = row[`Tables_in_${DB_NAME}`];
      const done      = this.async();

      console.log(`##QUERY::: Table name ${tableName}`);
      console.log('==========================================')

      if (CONFIG.ignoredTables.includes(tableName)) {
        console.log(`##QUERY::: Table ignored..`);
        console.log('==========================================')
        done();
        return;
      }

      const createIndexes = `ALTER TABLE ${tableName} ADD INDEX ('account_id(10)), ADD INDEX ('month')`;

      console.log(`##QUERY::: Creating indexes...`);
      db.query({sql: createIndexes, timeout: CONFIG.timeout}, (err) => {
        if (err) {
          return handleError(err, createIndexes);
        }

        console.log(`##QUERY::: Indexes created.`);
        console.log('==========================================')
        done();
      })
    }, allDone);
  });
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