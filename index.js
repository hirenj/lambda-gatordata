'use strict';
/*jshint esversion: 6, node:true */

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

exports.readAllData = require('./lambdas/readAllData').readAllData;

exports.runSplitQueue = require('./lambdas/runSplitQueue').runSplitQueue;

exports.startSplitQueue = require('./lambdas/startSplitQueue').startSplitQueue;
exports.endSplitQueue = require('./lambdas/endSplitQueue').endSplitQueue;
exports.stepSplitQueue = require('./lambdas/stepSplitQueue').stepSplitQueue;

exports.datasetCleanup = require('./lambdas/datasetCleanup').datasetCleanup;

exports.refreshData = dataloader.refreshData;
exports.refreshMetadata = dataloader.refreshMetadata;
exports.splitFiles = dataloader.splitFiles;