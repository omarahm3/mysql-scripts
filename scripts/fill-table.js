require('dotenv').config();

const {init, connect}     = require('./db-connection');
const {log}               = require('./helper');
const _cliProgress        = require('cli-progress');
const _randomTimestamp    = require('random-timestamps');
const _randomMillisecond  = require('random-millisecond');
const { Parser }          = require('json2csv');
const _path               = require("path");
const _fileSystem         = require("fs");
const CONFIG              = {
  timeout: process.env.QUERY_TIMEOUT,
  breakOnError: false,
  bulkValue: 1000000,
  loopValue: 1,
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
  
  const rows = await getUniqueData();

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
  const distinctQuery = `SELECT 
                        (SELECT group_concat(DISTINCT account_id) FROM test.report_conversations) as account_id,
                        (SELECT group_concat(DISTINCT assigned_agent_id) FROM test.report_conversations) as assigned_agent_id,
                        (SELECT group_concat(DISTINCT chatcampaign_id) FROM test.report_conversations) as chatcampaign_id,
                        (SELECT group_concat(DISTINCT session_id) FROM test.report_conversations) as session_id,
                        (SELECT group_concat(DISTINCT visitor_id) FROM test.report_conversations) as visitor_id,
                        (SELECT group_concat(DISTINCT host_id) FROM test.report_conversations) as host_id`
                        
  log('QUERY', `Executing query ${distinctQuery} \n`);

  const rows = prepareRows(await db.query({sql: distinctQuery, timeout: CONFIG.timeout}));

  // Save that result to file
  saveToFile(fileTitle, JSON.stringify(rows));

  return rows;
}

prepareRows = (rows) => {
  const row   = rows[0];
  const data  = {}

  data['account_id']        = row['account_id'].split(',');
  data['assigned_agent_id'] = row['assigned_agent_id'].split(',');
  data['chatcampaign_id']   = row['chatcampaign_id'].split(',');
  data['session_id']        = row['session_id'].split(',');
  data['visitor_id']        = row['visitor_id'].split(',');
  data['host_id']           = row['host_id'].split(',');
  return data;
}

generateCsvFile = (data, title = 'data.csv') => {
  return saveToFile(title, data);
}

saveToFile = (title, data) => {
  const file = _path.resolve(process.cwd(), title);
  
  if (_fileSystem.existsSync(file)) {
    return _fileSystem.appendFileSync(file, data);
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

  rows[elementName].forEach((value) => {
    // Discard empty values
    if (value == '' || value == 'undefined') {
      return;
    }
    elements.push(value);
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