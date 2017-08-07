'use strict';
/*jshint esversion: 6, node:true */

const zlib = require('zlib');

var bucket_name = 'test-gator';
var data_table = 'data';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    data_table = config.tables.data;
} catch (e) {
}

const AWS = require('lambda-helpers').AWS;

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const dataloader = require('./dataloader');
const dataremover = require('./dataremover');
const getdata = require('./getdata_dynamodb');

var get_homologues = function(accession) {
  return download_all_data(accession,{'homology/homology': ['*']},'homology')
         .then(function(homologues) {
            if (! homologues || ! homologues.data) {
              return {'homology' : []};
            }
            let family = homologues.data.family;
            homologues = {'homology' : homologues.data.homology.filter( (id) => id.toLowerCase() !== accession ) };
            return download_all_data(family, { 'homology/homology_alignment' : ['*']},'homology_alignment').then(function(alignments) {
              homologues.alignments = alignments;
              return homologues;
            });
         });
};

var download_all_data = function(accession,grants,dataset) {
  return getdata.download_all_data(accession,grants,dataset);
};

var combine_sets = function(entries) {
  if ( ! entries || ! entries.map ) {
    return entries;
  }
  var results = {"data" : [], "retrieved" : new Date().toISOString()};
  results.data = entries.map(function(entry) { return entry; });
  return results;
};

/*
Test event
{
  "acc": "Q9VZF9",
  "authorizationToken": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2Nlc3MiOnsiZm9vZ3JvdXAvdGVzdGluZyI6WyIqIl0sImZvb2dyb3VwLzBCeTQ4S0tEdTlsZUNjWE5hUXpaelZGOW1hR00iOlsiUDEyMzQ1Il19fQ.l3kBNPB1p5kXfobGhCoXP9taUvbbn3vloqQYgd3P6cA"
}
*/

var readAllData = function readAllData(event,context) {
  console.log("readAllData");
  console.log(event);
  var accession = event.acc.toLowerCase();

  var grants = event.grants ? JSON.parse(event.grants) : {};

  Object.keys(grants).forEach( (set) => {
    if (grants.proteins[ grants[set][0] ]) {
      grants[set] = grants.proteins[ grants[set][0] ];
    }
  });
  delete grants.proteins;

  event.dataset = event.dataset || '';
  var dataset = (event.dataset.indexOf(':') < 0 ) ? event.dataset : event.dataset.split(':')[1];

  // Decode JWT
  // Get groups/datasets that can be read
  let start_time = null;

  let entries_promise = Promise.resolve([]);

  if (event.homology) {
    entries_promise = getdata.datasetnames.then( () => get_homologues(accession)).then(function(homologue_data) {
      console.time('homology');
      let homologues = homologue_data.homology;
      let alignments = homologue_data.alignments;
      return Promise.all( [ Promise.resolve([alignments]) ].concat(homologues.map( homologue => download_all_data(homologue.toLowerCase(),grants,dataset) ) ) );
    })
    .then( (sets) => { console.timeEnd('homology'); console.time('combine_sets'); return sets; })
    .then( (entrysets) => entrysets.reduce( (a,b) => a.concat(b) ) );
  } else {
    console.time('download_all_data')
    entries_promise = getdata.datasetnames.then( () => download_all_data(accession,grants,dataset)).then(function(entries) {
      console.timeEnd('download_all_data')
      console.time('combine_sets');
      return entries;
    });
  }

  entries_promise
  .then(combine_sets).then(function(combined) {
    console.timeEnd('combine_sets');
    context.succeed(combined);
  }).catch(function(err) {
    console.error(err);
    console.error(err.stack);
    context.succeed('NOT-OK');
  });
};

exports.readAllData = readAllData;

exports.runSplitQueue = dataloader.runSplitQueue;

exports.startSplitQueue = dataloader.startSplitQueue;
exports.endSplitQueue = dataloader.endSplitQueue;
exports.stepSplitQueue = dataloader.stepSplitQueue;

exports.datasetCleanup = dataremover.datasetCleanup;

exports.refreshData = dataloader.refreshData;
exports.refreshMetadata = dataloader.refreshMetadata;
exports.splitFiles = dataloader.splitFiles;