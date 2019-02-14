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
const CACHING_MAX_RETRIES = parseInt(process.env.CACHING_MAX_RETRIES) || 300;
const FILE_STORAGE = process.env.FILE_STORAGE || '/data/files';
const CRON_FREQUENCY = process.env.CACHING_CRON_PATTERN || '0 */15 * * * *';

/**
<<<<<<< HEAD
* Internal Constants
*/
const DEFAULT_CONTENT_TYPE = 'text/plain';
const DEFAULT_EXTENSION = '.txt'
=======
* Internal constants
*/
const DEFAULT_EXTENSION = '.html';
>>>>>>> master

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
  fetchingJob();
  res.send(`Started. Repeating pattern: '${CRON_FREQUENCY}'`);
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
async function fetchingJob () {
  
  const fileAddresses = await getFileAddressToDo(CACHING_MAX_RETRIES);
  
  //--- start the process of downloading the resources
  const promises = fileAddresses.map( async (fileAddress) => {
    
    const uri = fileAddress.uri.value;
    const url = fileAddress.url.value;
    const timesTried = fileAddress.hasOwnProperty('timesTried') ? parseInt(fileAddress.timesTried.value) : 0;

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
      await setStatus(uri, getStatusLabelFor(timesTried), null, timesTried + 1);
      return;
    }

    if (downloadResult.successful) {
      try {
        console.log(`Associating ${uri}`);
        console.log(`            ${url}`);
        //--- associate the downloaded file to the fileAddress
        associationResult = await associateCachedFile(downloadResult);
      }
      catch (err) {
        //--- The file has been successfuly deleted but it could not be associated
        //--- with the FileAddress object in the database, maybe for some database error.
        //--- We need to clean up
        cleanUpFile(downloadResult.cachedFileAddress);
        //--- Since this failure was not due to the remote server, we will try it again
        //--- So, we don't inrease the timesTried value
        await setStatus(uri, FAILED, null, timesTried);
        return;
      }
    } else {
      //--- Due to an error on the remote resource side, the file could not be downloaded
      //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
      await setStatus(uri, getStatusLabelFor(timesTried), parseInt(downloadResult.result.statusCode), timesTried + 1);
      return;
    }

    //--- File was successfuly downloaded and cached
    //--- update the cachedStatus of the fileAddress to CACHED
    await setStatus(uri, CACHED, parseInt(downloadResult.result.statusCode), timesTried + 1);
    console.log (`${url} is cached successfuly`);
  });
}

function getStatusLabelFor (times) { 
  let val = times + 1 < CACHING_MAX_RETRIES ? FAILED : DEAD;
  return val;
}

function makeFileName() {
  return uuid();
}

async function downloadFile (fileAddress) {

  return new Promise((resolve, reject) => {

    const uri = fileAddress.uri.value;
    const url = fileAddress.url.value;
    let r = request(url);

    r.on('response', (resp) => {
      //check things about the response here.
      const code = resp.statusCode;

      //Note: by default, redirects are followed :-)
      if (200 <= code && code < 300) {
<<<<<<< HEAD
        //--- OK
        //--- write the file
        const mimeType = resp.headers['content-type'] || DEFAULT_CONTENT_TYPE;
        let extension = mime.extension(mimeType) || DEFAULT_EXTENSION;
=======
        //--- Status: OK
        //--- create file attributes
        let extension = getExtensionFrom(resp.headers);
>>>>>>> master
        let bareName = makeFileName();
        let physicalFileName = [bareName, extension].join('.');
        let localAddress = path.join(FILE_STORAGE, physicalFileName);

        //--- write the file
        r.pipe(fs.createWriteStream(localAddress))
          .on('error', err => {
            //--- We need to clean up on error during file writing
            console.log (`${localAddress} failed writing to disk, cleaning up...`);
            cleanUpFile(localAddress);
            reject({resource: fileAddress, error: err});
          })
          .on('finish', () => {
            resolve({
                  successful: true,
                  resource: fileAddress,
                  result: resp,
                  cachedFileAddress: localAddress,
                  cachedFileName: physicalFileName,
                  bareName: bareName,
                  extension: extension
            });
          });
      }
      else {
        //--- NO OK
        resolve({ successful: false, resource: fileAddress, result: resp });
      }
    });

    r.on('error', (err) => {
      console.log("Error while downloading a remote resource:");
      console.log(`  remote resource: ${uri}`);
      console.log(`  remote url: ${url}`);
      console.log(`  error: ${err}`);
      reject({resource: fileAddress, error: err});
    });
  });
}

async function associateCachedFile (downloadResult) {

  const uri = downloadResult.resource.uri.value;
  const name = downloadResult.cachedFileName;
  const extension = downloadResult.extension;
  const date = Date.now();

  //--- get the file's size
  const stats = fs.statSync(downloadResult.cachedFileAddress);
  const fileSize = stats.size;

<<<<<<< HEAD
  try {

    let contentType = headers['content-type'] || DEFAULT_CONTENT_TYPE;
    let created = Date.now();
    let extension = downloadResult.extension; 

=======
  //--- read data from HTTP response heades
  const headers = downloadResult.result.headers;
  const contentType = getContentTypeFrom(headers);

  try {
>>>>>>> master
    //First create the virtual file.
    let fileObjectUri = FILE_RESOURCES_PATH + uuid(); //WE assume trailing slash
    let result = await createVirtualFileDataObject(fileObjectUri,
                                                  uri,
                                                  name,
                                                  contentType,
                                                  fileSize,
                                                  extension,
<<<<<<< HEAD
                                                  created);

=======
                                                  date);
>>>>>>> master
    //create the physical file
    let physicalUri = 'share://' + downloadResult.cachedFileName; //we assume filename here
    let resultPhysicalFile = await createPhysicalFileDataObject(physicalUri,
                                                                fileObjectUri,
                                                                name,
                                                                contentType,
                                                                fileSize,
                                                                extension,
<<<<<<< HEAD
                                                                created);
=======
                                                                date);
>>>>>>> master
  }
  catch (err) {
    console.log('Error while associating a downloaded file to a FileAddress object');
    console.log(err);
    console.log(`  downloaded file: ${downloadResult.cachedFileAddress}`);
    console.log(`  FileAddress object: ${uri}`);
    throw err;
  }
}

function cleanUpFile (path){
  if(fs.existsSync(path)){
    fs.unlinkSync(path);
  }
}

function getContentTypeFrom(headers) {
  return headers['content-type'] || 'text/plain';
}

function getExtensionFrom(headers) {
  const mimeType = headers['content-type'];
  return mime.extension(mimeType) || DEFAULT_EXTENSION;
}