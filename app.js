import { app, query, errorHandler, uuid } from 'mu';
import request from 'request';
import fs  from 'fs-extra';
import mime from 'mime-types';

const CACHING_MAX_RETRIES = process.env.CACHING_MAX_RETRIES || 3;
const FILE_STORAGE = process.env.FILE_STORAGE || '/data/files';

app.get('/', function( req, res ) {
  res.send('Hello mu-javascript-template');
} );

app.get('/checkurls', async function( req, res ) {
  const q = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?uri ?url ?times ?statusLabel {

      ?uri a ext:FileAddress ;
          ext:fileAddress ?url .

      OPTIONAL { 
        ?uri ext:fileAddressCacheStatus ?statusUri .
        ?statusUri ext:fileAddressCacheStatusTimesRetried ?times .
      }

      OPTIONAL { 
        ?uri ext:fileAddressCacheStatus ?statusUri .
        ?statusUri ext:fileAddressCacheStatusLabel ?statusLabel.
      }
      FILTER (?statusLabel != "cached" && (!BOUND(?times) || ?times < ${CACHING_MAX_RETRIES}))
    }`;

  let response = await query(q);
  let fileAddresses = response.results.bindings;
  
  let promises = fileAddresses.map( async (fileAddress) => {
    //--- download the content from fileAddress
    let downloadResult = await downloadFile(fileAddress);
    //--- associate the downloaded file to the fileAddress
    await associateCachedFile(downloadResult);
    //--- update the cachedStatus of the fileAddress
    await updateStatus(downloadResult);
  });

  await Promise.all(promises);
  res.send("Done");
});

app.use(errorHandler);

const updateStatus = async function (downloadResult) {

  const code = downloadResult.result.hasOwnProperty('statusCode') ? parseInt(downloadResult.result.statusCode) : null;
  const status = code === 200 ? "cached" : "pending";
  const times = (downloadResult.resource.hasOwnProperty('times') ? parseInt(downloadResult.resource.times.value) : 0) + 1;
  const uid = uuid();
  const uri = downloadResult.resource.uri.value;
  const statusResourcePath = "http://data.lblod.info/file-address-cache-statuses";

  return  await query( `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    DELETE { 
      GRAPH ?g { 
        <${uri}> ext:fileAddressCacheStatus ?status.
      }
    } 
    WHERE { 
      GRAPH ?g { 
        ?fileAddress ext:fileAddressCacheStatus ?status 
      } 
    };

    INSERT { 
      GRAPH ?g { 
        ?statusUri a 
            ext:FileAddressCacheStatus; 
            ext:fileAddressCacheStatusLabel "${status}";
            ${code != null ? `ext:fileAddressCacheStatusHttpStatus "${code}"^^xsd:integer;` : ''}
            ext:fileAddressCacheStatusTimesRetried "${times}"^^xsd:integer;
            <http://mu.semte.ch/vocabularies/core/uuid> "${uid}".
        <${uri}> ext:fileAddressCacheStatus ?statusUri. 
      } 
    } 
    WHERE { 
      GRAPH ?g { 
        ?s a ext:FileAddress 
      } 
      BIND(IRI("${statusResourcePath}/${status}/${uid}") as ?statusUri).
    }
  `);
}

const downloadFile = async function (fileAddress) {
  return new Promise((resolve, reject) => {
    let r = request(fileAddress.url.value);

    r.on('response', (resp) => {
      //check things about the response here.
      const mimeType = resp.headers['content-type'];
      //write the file
      let localAddress = `${FILE_STORAGE}/${uuid()}.${mime.extension(mimeType)}`;
      r.pipe(fs.createWriteStream(localAddress));
      resolve({resource: fileAddress, result: resp, cachedFileAddress:localAddress});
    });

    r.on('error', (err) => {
      resolve({resource: fileAddress, result: err}); //you could should call reject() if you need to do something with the error later.
    });
  });
}

const associateCachedFile = async function (downloadResult) {
  if (! downloadResult.hasOwnProperty('cachedFileAddress')) {
    return null;
  }

  //--- make a file resource
  const address = downloadResult.cachedFileAddress;
  const fileResourcePath = "http://data.lblod.info/files/";
  const uid = uuid();
  try {
    let fileCreationResult = await query(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX dct: <http://purl.org/dc/terms/>

      INSERT { 
        GRAPH ?g {
          # make a file resource
          ?file a 
              nfo:FileDataObject;
              nfo:fileName "${uid}";
              dct:format "${downloadResult.result.headers['content-type']}";
              nfo:fileSize "${downloadResult.result.headers['content-length']}";
              dbpedia:fileExtension "${downloadResult.result.headers['content-type'].split('/')[1]}";
              nfo:fileCreated "${downloadResult.result.headers['date']}";
              <http://mu.semte.ch/vocabularies/core/uuid> "${uid}".

          # associate it to our original FileAddress object
          <${downloadResult.resource.uri.value}> nie:dataSource ?file. 
        } 
      } 
      WHERE { 
        GRAPH ?g { 
          ?s a ext:FileAddress 
        } 
        BIND(IRI("${fileResourcePath}/${uid}") as ?file).
      }
    `);

    return fileCreationResult;
  }
  catch (err) {
    return null;
  }

  const cronFrequency = process.env.CACHING_CRON_PATTERN || '*/30 * * * * *';
  new CronJob(cronFrequency, function() {
    console.log(`Download-url service triggered by cron job at ${new Date().toISOString()}`);
    deliverPackages();
  }, null, true);
}




