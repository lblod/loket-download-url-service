import { query, uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate } from 'mu';

/**
 * SPARQL constants
 */
const UUID_URI = '<http://mu.semte.ch/vocabularies/core/uuid>';
const EXT_PREFIX = 'ext: <http://mu.semte.ch/vocabularies/ext/>';
const NFO_PREFIX = 'nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>';
const NIE_PREFIX = 'nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>';
const DCT_PREFIX = 'dct: <http://purl.org/dc/terms/>';
const STATUS_RESOURCES_PATH = "http://data.lblod.info/file-address-cache-statuses";
const FILE_RESOURCES_PATH = "http://data.lblod.info/files/";
/**
 * States of a FileAddress object
 * PENDING : is being downloaded
 * FAILED : last download has failed
 * CACHED : has been successfully cached
 * DEAD : has been tried for the maximum allowed times
 */
const PENDING = 'pending';
const FAILED = 'failed';
const CACHED = 'cached';
const DEAD = 'dead';

const getFileAddressToDo = async function( caching_max_retries ) {
  //--- get a list of all failed FileAddress objects
  let q = `
    PREFIX ${EXT_PREFIX}

    SELECT ?uri ?url ?timesTried ?statusLabel {

      ?uri a ext:FileAddress ;
          ext:fileAddress ?url .
      
      ?toezicht toezicht:fileAddress ?uri ;
        adms:status ?docStat .

      ?docStat skos:prefLabel ?docStatLabel .

      OPTIONAL {
        ?uri ext:fileAddressCacheStatus ?statusUri .
        ?statusUri ext:fileAddressCacheStatusTimesRetried ?timesTried .
      }

      OPTIONAL {
        ?uri ext:fileAddressCacheStatus ?statusUri .
        ?statusUri ext:fileAddressCacheStatusLabel ?statusLabel .
      }

      FILTER (
        (?docStatLabel = 'verstuurd')
        &&
        (!BOUND(?statusLabel) || ?statusLabel = ${sparqlEscapeString(FAILED)})
        &&
        (!BOUND(?timesTried) || ?timesTried < ${sparqlEscapeInt(caching_max_retries)})
      )
    }
  `;
   return await query(q);
};

const setStatus = async function (uri, statusLabel, responseCode = null, timesTried = 0) {

  console.log(`Setting ${statusLabel} status for ${uri}`);

  const uid = uuid();

  let q = `
    PREFIX ${EXT_PREFIX}

    DELETE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} ext:fileAddressCacheStatus ?status .
        ?status ?p ?o .
      }
    }
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} ext:fileAddressCacheStatus ?status .
        ?status ?p ?o .
      }
    }

    ;

    INSERT {
      GRAPH ?g {
        ?statusUri a
            ext:FileAddressCacheStatus ;
            ext:fileAddressCacheStatusLabel ${sparqlEscapeString(statusLabel)} ;
            ${responseCode != null ? `ext:fileAddressCacheStatusHttpStatus ${sparqlEscapeInt(responseCode)} ;` : ''}
            ext:fileAddressCacheStatusTimesRetried ${sparqlEscapeInt(timesTried)};
            ${UUID_URI} ${sparqlEscapeString(uid)} .
        ${sparqlEscapeUri(uri)} ext:fileAddressCacheStatus ?statusUri .
      }
    }
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a ext:FileAddress.
      }
      BIND(IRI(${sparqlEscapeString([STATUS_RESOURCES_PATH, statusLabel, uid].join('/'))}) as ?statusUri).
    }
  `;

  try {
    return await query( q );
  }
  catch (err) {
    console.log(`Error while setting ${statusLabel} status`);
    console.log(` resource: ${uri}`);
    console.log(` error: ${err}`);
    throw err;
  }
};

const createVirtualFileDataObject = async function(fileObjectUri, fileAddressUri, name, type, fileSize, extension, created){
  const uid = uuid();
  let q = `
    PREFIX ${EXT_PREFIX}
    PREFIX ${NFO_PREFIX}
    PREFIX ${NIE_PREFIX}
    PREFIX ${DCT_PREFIX}

    INSERT {
      GRAPH <http://mu.semte.ch/graphs/public> {
        # make a file resource
        ${sparqlEscapeUri(fileObjectUri)} a
            nfo:FileDataObject;
            nfo:fileName ${sparqlEscapeString(name)};
            dct:format ${sparqlEscapeString(type)};
            nfo:fileSize ${sparqlEscapeInt(fileSize)};
            dbpedia:fileExtension ${sparqlEscapeString(extension)};
            nfo:fileCreated ${sparqlEscapeDate(created)};
            ${UUID_URI} ${sparqlEscapeString(uid)}.

        # associate it to our original FileAddress object
        ${sparqlEscapeUri(fileAddressUri)} nie:dataSource  ${sparqlEscapeUri(fileObjectUri)}.

        #HACK for the sprintf issue
        ${sparqlEscapeUri(fileAddressUri)} ?p ?o.
      }
    }
  `;
  return await query( q );
};

const createPhysicalFileDataObject = async function(fileObjectUri, dataSourceUri, name, type, fileSize, extension, created){
  //TODO: merge with pervious query
  const uid = uuid();
  let q = `
    PREFIX ${EXT_PREFIX}
    PREFIX ${NFO_PREFIX}
    PREFIX ${NIE_PREFIX}
    PREFIX ${DCT_PREFIX}

    INSERT {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(fileObjectUri)} a nfo:FileDataObject;
              nfo:fileName ${sparqlEscapeString(name)};
              nie:dataSource ${sparqlEscapeUri(dataSourceUri)};
              ${UUID_URI} ${sparqlEscapeString(uid)};
              dct:format ${sparqlEscapeString(type)};
              nfo:fileSize ${sparqlEscapeInt(fileSize)};
              dbpedia:fileExtension ${sparqlEscapeString(extension)};
              nfo:fileCreated ${sparqlEscapeDate(created)}.

        #HACK for the sprintf issue
        ${sparqlEscapeUri(dataSourceUri)} ?p ?o.
      }
    }
  `;
  return await query( q );
};

export { createVirtualFileDataObject, createPhysicalFileDataObject, getFileAddressToDo, setStatus, PENDING, FAILED, CACHED, DEAD, FILE_RESOURCES_PATH}
