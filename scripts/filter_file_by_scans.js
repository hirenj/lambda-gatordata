'use strict';
/*jshint esversion: 6, node:true */

const fs = require('fs');
const JSONStream = require('JSONStream');

const MetadataExtractor = require('../MetadataExtractor').MetadataExtractor;

const retrieve_file_local = function retrieve_file_local(filekey) {
  return fs.createReadStream(filekey);
};

async function read(stream) {
	const chunks = [];
	for await (const chunk of stream) chunks.push(chunk); 
	return Buffer.concat(chunks).toString('utf8').split('\n').filter( line => line ).map( line => line.trim().split('\t'));
}

function pretty_print(data,depth) {
  let tab_size = 2;
  let indent = Array(tab_size+1).join(' ');
  let output = JSON.stringify(data,null,indent);
  let start_re = new RegExp('\n {'+(depth+1)*tab_size+',}','gm');
  let end_re = new RegExp('([\\"\\}\\]])\\n {'+(depth*tab_size)+'}','gm');
  return output.replace(start_re,'').replace(end_re,'$1');
}


let filename = process.argv[2];

if ( ! filename ) {
	console.log('Usage: tsv with "uniprot_col\tscore_col\tscan_col" | node filter_file_by_scans.js input.msdata.json > output.json ');
	process.exit(0);
}

let rs = retrieve_file_local(filename);

let entry_data = rs.pipe(JSONStream.parse(['data', {'emitKey': true}]));
let metadata_ready = new Promise(resolve => {
	rs.pipe(new MetadataExtractor()).on('data',resolve);
});

read(process.stdin).then( scans => {
	let test_map = {};
	let wanted_uniprots = [];
	for (let scan of scans) {
		let uniprot = scan[0];
		let pep_scores = scan[1].split(',');
		let pep_scans = scan[2].split(',');
		pep_scores.forEach( (score,idx) => {
			test_map[ parseFloat(score).toFixed(2) + pep_scans[idx].replace(/^[^-]+/,'') ] = 1;			
		});
		wanted_uniprots.push(uniprot);
	}
	let filtered_data = {};
	entry_data.on('data', dat => {
		let val;
		let wanted_data = [];
		if ( wanted_uniprots.indexOf(dat.key) < 0) {
			return;
		}
		for (let peptide of dat.value) {
			let wanted_spectra = [];
			let passing_ids = [];
			for (let spec of peptide.spectra) {
				if (test_map[spec.score.toFixed(2)+'-'+spec.scan]) {
					test_map[spec.score.toFixed(2)+'-'+spec.scan]+= 1;
					passing_ids.push(spec.score+'-'+spec.scan);
					wanted_spectra.push(spec);
				}
			}
			if (wanted_spectra.length > 0) {
				peptide.spectra = wanted_spectra;
				wanted_data.push(peptide);
			}
		}
		if (wanted_data.length > 0) {
			filtered_data[dat.key] = wanted_data;
		}
	});
	entry_data.on('end', () => {
		for (let key of Object.keys(test_map)) {
			if (test_map[key] == 1) {
				throw new Error(`Did not map spectra from ${key}`);
			}
		}
		metadata_ready.then( metadata => {
			metadata.software.push({version: "",name: "hirenj/lambda-gatordata/filter_file_by_scans", rundate: (new Date()).toISOString()});
			metadata.original_title = metadata.title;
			metadata.title = `${metadata.title} Filtered for Scans`;
			console.log(pretty_print({data: filtered_data, metadata },2));
		});
	});


})

