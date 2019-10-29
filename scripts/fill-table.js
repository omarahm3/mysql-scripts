require('dotenv').config();

const {init, connect}              = require('./db-connection');
const {log}               = require('./helper');
const _forEach            = require('async-foreach').forEach;
const _cliProgress        = require('cli-progress');
const _randomTimestamp    = require('random-timestamps');
const _randomMillisecond  = require('random-millisecond');
const { _csvParser }      = require('json2csv');
const _fileSystem         = require("fs");
const CONFIG              = {
  timeout: process.env.QUERY_TIMEOUT,
  breakOnError: false,
  bulkValue: 50000,
  loopValue: 500,
  columns: [
    'acceptUrl',
    'account_id',
    'assigned_agent_id',
    'avgMessageLength',
    'avgResponseTime',
    'chatcampaign_id',
    'conversation_start',
    'decisionTime',
    'duration_afterclose_secs',
    'duration_conversation_secs',
    'duration_total_secs',
    'firstReplyTime',
    'firstResponseTime',
    'host_id',
    'last_agent_id',
    'maxMessageLength',
    'maxResponseTime',
    'num_messages',
    'requestSource',
    'requestType',
    'session_id',
    'successcode',
    'survey_comment',
    'survey_result',
    'transcriptUrl',
    'visitor_id',
    'visitor_identifier',
  ]
}

let db                = {};
let accountIDs        = [];
let assignedAgentsIDs = [];
let chatCampaignIDs   = [];
let sessionIDs        = [];
let visitorIDs        = [];
let hostIDs           = [];
let insertValeusBar   = {};

main = async () => {
  db = await connect();

  log('SCRIPT_LOG', 'Connected to database', '-', 30, true, true);
  
  // First get all unique account IDs to insert records with random account ids
  const uniqueAccountIds = `SELECT DISTINCT account_id, assigned_agent_id, chatcampaign_id, session_id, visitor_id, host_id FROM test.report_conversations`;
  log('QUERY', `Executing query ${uniqueAccountIds} \n`);

  const rows = await db.query({sql: uniqueAccountIds, timeout: CONFIG.timeout});

  console.log(JSON.parse)

  log('QUERY', `Processing insert`);

  await processInsert(rows);

  log('SCRIPT_LOG', `Script finished`);
  process.exit();
}

main();

processInsert = async (rows) => {
  for (let i=0 ; i<CONFIG.loopValue ; i++) {

    accountIDs        = getFromMysqlRows(rows, 'account_id');
    assignedAgentsIDs = getFromMysqlRows(rows, 'assigned_agent_id');
    chatCampaignIDs   = getFromMysqlRows(rows, 'chatcampaign_id');
    sessionIDs        = getFromMysqlRows(rows, 'session_id');
    visitorIDs        = getFromMysqlRows(rows, 'visitor_id');
    hostIDs           = getFromMysqlRows(rows, 'host_id');

    log('QUERY', `-- Processing batch #${i+1}\n`);

    insertValeusBar   = new _cliProgress.SingleBar({}, _cliProgress.Presets.shades_classic);
    const values      = generateInsertValues(CONFIG.bulkValue);
    const parser      = new Parser({fields: CONFIG.columns});
    const parsedVals  = parser.parse(values);

    generateCsvFile(parsedVals);


    // const insertQuery = `INSERT INTO test.report_conversations (${CONFIG.columns.join()}) VALUES ?`;

    // await db.query({sql: insertQuery, timeout: CONFIG.timeout}, [values]);

    insertValeusBar.stop();

    log('\nQUERY', `-- Batch #${i+1} inserted`);
  }
}

getUniqueData = async () => {
  const fileTitle = 'unique-columns.txt';

  if (_fileSystem.existsSync(_path.resolve(process.cwd(), fileTitle))) {
    const result = _fileSystem.readFileSync(fileTitle, 'utf8');
    return JSON.parse(result);
  }
  // First get all unique account IDs to insert records with random account ids
  const uniqueAccountIds = `SELECT DISTINCT account_id, assigned_agent_id, chatcampaign_id, session_id, visitor_id, host_id FROM test.report_conversations`;
  log('QUERY', `Executing query ${uniqueAccountIds} \n`);

  const rows = await db.query({sql: uniqueAccountIds, timeout: CONFIG.timeout});

  // Save that result to file
  saveToFile(fileTitle, JSON.stringify(rows));

  return rows;
}

generateCsvFile = (data, title = 'data.csv') => {
  return saveToFile(title, data);
}

saveToFile = (title, data) => {
  const file = _path.resolve(process.cwd(), title);
  
  if (_fileSystem.existsSync(file)) {
    _fileSystem.appendFileSync(file, data);
  }

  return _fileSystem.writeFileSync(file, data, 'utf8');
}

generateInsertValues = (count) => {
  insertValeusBar.start(count, 0);

  const values              = [];
  const transcriptUrl       = 'https://staging.cobrowser.io/manager/#/transcriptdetail/SHRHEjM0qEzne9gqDp0kMDdCO6AvdQuh';
  const acceptUrl           = 'https://vic-staging.jimdo.com/vic-inv/';

  while(count-- > 0 ) {
    const ractiveVsProactive  = getRandomFromArray(['reactive', 'proactive']);
    const agentId             = getRandomFromArray(assignedAgentsIDs);
    const accountId           = getRandomFromArray(accountIDs);
    const chatcampaignId      = getRandomFromArray(chatCampaignIDs);
    const sessionId           = getRandomFromArray(sessionIDs);
    const visitorId           = getRandomFromArray(visitorIDs);
    const hostId              = getRandomFromArray(visitorIDs);

    values.push({
      acceptUrl: acceptUrl,                                // acceptUrl
      account_id: accountId,                                // account_id
      assigned_agent_id: agentId,                                  // assigned_agent_id
      avgMessageLength: _randomMillisecond({ min: 1, max: 100 }),  // avgMessageLength
      avgResponseTime: _randomMillisecond({ min: 1, max: 100 }),  // avgResponseTime
      chatcampaign_id: chatcampaignId,                           // chatcampaign_id
      conversation_start: _randomTimestamp(),                        // conversation_start
      decisionTime: _randomMillisecond({ min: 1, max: 10 }),   // decisionTime
      duration_afterclose_secs: _randomMillisecond({ min: 1, max: 60 }),   // duration_afterclose_secs
      duration_conversation_secs: _randomMillisecond({ min: 1, max: 25 }),   // duration_conversation_secs
      duration_total_secs: _randomMillisecond({ min: 100, max: 500 }),// duration_total_secs
      firstReplyTime: _randomMillisecond({ min: 1, max: 30 }),   // firstReplyTime
      firstResponseTime: _randomMillisecond({ min: 1, max: 30 }),   // firstResponseTime
      host_id: hostId,                                     // host_id
      last_agent_id: agentId,                                    // last_agent_id
      maxMessageLength: _randomMillisecond({ min: 1, max: 100 }),   // maxMessageLength
      maxResponseTime: _randomMillisecond({ min: 1, max: 100 }),   // maxResponseTime
      num_messages: _randomMillisecond({ min: 1, max: 10 }),    // num_messages
      requestSource: 'Footer standaard knop',                    // requestSource
      requestType: ractiveVsProactive,                       // requestType
      session_id: sessionId,                                // session_id
      successcode: '',                                       // successcode
      survey_comment: '',                                       // survey_comment
      survey_result: '',                                       // survey_result
      transcriptUrl: transcriptUrl,                            // transcriptUrl
      visitor_id: visitorId,                                // visitor_id
      visitor_identifier: '',                                       // visitor_identifier               
    });

    insertValeusBar.increment();
  }

  return values;
}

getRandomFromArray = (arr) => {
  return arr[Math.floor(Math.random()*arr.length)]
}

getFromMysqlRows = (rows, elementName) => {
  const elements = [];

  rows.forEach((row) => {
    // Discard empty values
    if (row[elementName] == '') {
      return;
    }
    elements.push(row[elementName]);
  })

  return elements;
}

function handleError(err, query) {
  // log('SCRIPT_ERROR', `Error while running query "${query}"`);
  // log('SCRIPT_ERROR', `Error details = ${JSON.stringify(err)}`);
  console.log(err)
  if (CONFIG.breakOnError) {
    process.exit()
  }
}

function allDone(notAborted, arr) {
  log('SCRIPT_LOG', 'All tables are done.')
  process.exit()
}