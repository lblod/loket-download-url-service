import { app, query, errorHandler, uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate } from 'mu';
import { createVirtualFileDataObject, createPhysicalFileDataObject, getFileAddressToDo, setStatus, PENDING, FAILED, CACHED, DEAD, FILE_RESOURCES_PATH } from './queries';
import { CronJob } from 'cron';
import request from 'request';
import fs  from 'fs-extra';
import mime from 'mime-types';
import path from 'path';

/**
* Environment constants
*/
const CACHING_MAX_RETRIES = process.env.CACHING_MAX_RETRIES || 300;
const FILE_STORAGE = process.env.FILE_STORAGE || '/data/files';
const CRON_FREQUENCY = process.env.CACHING_CRON_PATTERN || '0 */15 * * * *';


app.get('/', function( req, res ) {
  res.send(`
Welcome to the dowload url service.

This service periodically looks for urls and tries to download and store their content locally, if not already stored. You can force it to run immediately by visiting /checkurls subroute. This service utilizes these environment variables:
    
  FILE_STORAGE
    The local storage of files

  CACHING_MAX_RETRIES
    How many times will the service try to download a resource before considering it as failed.

  CACHING_CRON_PATTERN
    The time interval of service's re-execution.`);
});

/**
 * This route can be used to force an immidiate run of the service
 */
app.get('/checkurls', async function( req, res ){
  await fetchingJob();
  res.send(`Started.
  Repeating pattern: ${CRON_FREQUENCY}`);
});

app.use(errorHandler);

/*
new CronJob(CRON_FREQUENCY, async function() {
  console.log(`Download-url service triggered by cron job at ${new Date().toISOString()}`);
  await fetchingJob();
}, null, true);
*/

//--- List of available statuses
//------ pending
//------ failed
//------ cached
//------ dead
const fetchingJob = async function() {

  let response = await getFileAddressToDo();
  let fileAddresses = response.results.bindings;

  //--- start the process of downloading the resources
  let promises = fileAddresses.map( async (fileAddress) => {

    const uri = fileAddress.uri.value;
    const url = fileAddress.url.value;
    const timesTried = fileAddress.hasOwnProperty('timesTried') ? fileAddress.timesTried : 0;

    let downloadResult = null;
    let associationResult = null;

    console.log(`Enqueuing ${url}`);

    try {
      //--- setting fileAddress's status to 'downloading' prevents us from
      //--- redownloading a resource in case it's download
      //--- takes longer than our iteration interval
      await setStatus (uri, PENDING, null, timesTried);
    }
    catch (err) {
      return;
    }
  
    try {
      //--- download the content of fileAddress
      downloadResult = await downloadFile(fileAddress);
    }
    catch (err) {
      //--- A connection to the remote resource was not established
      //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
      await setStatus(uri, statLabel(timesTried), null, timesTried + 1);
      return;
    }
    
    if (downloadResult.successfull) {
      try {
        console.log(`Associating ${uri}`);
        console.log(`            ${url}`);
        //--- associate the downloaded file to the fileAddress
        associationResult = await associateCachedFile(downloadResult);
      }
      catch (err) {
        //--- The file has been successfully deleted but it could not be associated
        //--- with the FileAddress object in the database, maybe for some database error.
        //--- We need to clean up
        cleanUpFile(downloadResult.cachedFileAddress);
         console.log('hello')
        //--- Since this failure was not due to the remote server, we will try it again
        //--- So, we don't inrease the timesTried value
        await setStatus(uri, FAILED, null, timesTried);
        return;
      }
    } else {
      //--- Due to an error on the remote resource side, the file could not be downloaded
      //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
      await setStatus(uri, statLabel(timesTried), parseInt(downloadResult.result.statusCode), timesTried + 1);
      return;
    }

    //--- File was successfully downloaded and cached
    //--- update the cachedStatus of the fileAddress to CACHED
    await setStatus(uri, CACHED, parseInt(downloadResult.result.statusCode), timesTried + 1);
    console.log (`${url} is cached successfully`);
  });
};

const statLabel = function (times) { return times + 1 < CACHING_MAX_RETRIES ? FAILED : DEAD; }


const makeFileName = function() {
  return uuid();
}
const downloadFile = async function (fileAddress) {

  return new Promise((resolve, reject) => {

    const uri = fileAddress.uri.value;
    const url = fileAddress.url.value;
    let r = request(url);

    r.on('response', (resp) => {
      //check things about the response here.
      const code = resp.statusCode;
      
      if (200 <= code && code < 300) {
        //--- OK
        //--- write the file
        const mimeType = resp.headers['content-type'];
        let extension = mime.extension(mimeType) || '.txt';
        let bareName = makeFileName();
        let physicalFileName = [bareName, extension].join('.');
        let localAddress = path.join(FILE_STORAGE, physicalFileName);

        r.pipe(fs.createWriteStream(localAddress));

        resolve({
          successfull: true,
          resource: fileAddress,
          result: resp, 
          cachedFileAddress: localAddress, 
          cachedFileName: physicalFileName, 
          bareName: bareName, 
          extension: extension
        });
      }
      else {
        //--- NO OK
        resolve({ successfull: false, resource: fileAddress, result: resp });
      }
    });

    r.on('error', (err) => {
      console.log("Error while downloading a remote resource:");
      console.log(`  remote resource: ${uri}`);
      console.log(`  remote url: ${url}`);
      console.log(`  error: ${err}`)
      reject({resource: fileAddress, error: err});
    });
  });
}

const associateCachedFile = async function (downloadResult) {

  const uid = uuid();
  const uri = downloadResult.resource.uri.value;
  const name = downloadResult.cachedFileName;
  const fileResourcePath = "http://data.lblod.info/files/";
  const headers = downloadResult.result.headers;

  //--- get the file's size
  const stats = fs.statSync(downloadResult.cachedFileAddress);
  const fileSize = stats.size;

  try {
    let fileCreationResult = await query( q );
    return fileCreationResult;
  }
  catch (err) {
    console.log('Error while associating a downloaded file to a FileAddress object');
    console.log(`  downloaded file: ${downloadResult.cachedFileAddress}`);
    console.log(`  FileAddress object: ${uri}`);
    throw err;
  }
}


const cleanUpFile = function(path){
  if(fs.existsSync(path)){
    fs.unlinkSync(path);
  }
};
