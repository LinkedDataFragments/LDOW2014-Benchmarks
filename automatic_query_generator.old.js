/**
 * @author Miel Vander Sande
 *
 * Script for creating automatic benchmark queries based on endpoint
 */

var request = require('request'), fs = require('fs'), n3 = require('n3');

/*
 * Define constants
 */
var endpoint = 'http://dbpedia.restdesc.org/?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=';
var count = 10;
var outputfile = '/Users/mielvandersande/Desktop/Projects/ldf/benchmarks/queries/output.log';

function start() {
	console.log('Query generator started!')

	executeQuery('SELECT DISTINCT ?type WHERE { ?s a ?type }', function(types) {
		setInterval(function() {
			if (count > 0) {
				//for (var i = 0; i<count; i++) {
				buildQuery(types);
			}
		}, 400);
	});
}

function addslashes(str) {
	return (str + '').replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
}

function buildQuery(types) {
	// 1. SELECT A RANDOM TYPE
	var selected_type = pickRandom(types, 1);
	if (selected_type.length > 0) {
		//Add to bgp
		var bgp = '?1 a <' + selected_type[0].type.value + '>; ';

		// 2. SELECT A RANDOM SUBJECT OF THAT TYPE
		executeQuery('SELECT DISTINCT ?s WHERE { ?s a <' + selected_type[0].type.value + '>}', function(subjects) {
			var selected_s = pickRandom(subjects, 1);
			if (selected_s.length > 0) {
				// 3. SELECT A RANDOM PREDICATE OF THAT SUBJECT
				var predicates = executeQuery('SELECT DISTINCT ?p WHERE { <' + selected_s[0].s.value + '> ?p ?o}', function(predicates) {
					var selected_p = pickRandom(predicates, 1);

					if (selected_p[0].length > 0) {
						//Add to bgp
						bgp += '<' + selected_p[0].p.value + '> ?2 . ';

						// 3. SELECT DISTINCT ?o WHERE {?1 a <1>; <2> ?o}

						// ?1 a <1>; <2> ?2 .
						// ?o => <Oi>

						executeQuery('SELECT DISTINCT ?o WHERE { <' + selected_s[0].s.value + '> <' + selected_p[0].p.value + '> ?o}', function(objects) {

							if (objects.length > 1) {

								objects = pickRandom(objects, 2);

								objects.forEach(function(selected_o) {

									if (selected_o[0].o.type == 'uri') {

										executeQuery('SELECT DISTINCT ?p, ?o WHERE { <' + selected_o[0].o.value + '> ?p ?o} ', function(terms) {

											var selected_term = pickRandom(terms, 1);

											if (selected_term.length > 0) {

												/*if (selected_term.o.type != 'uri') {

												 if (selected_term.o['xml:lang']) {
												 bgp += '?2 <' + selected_term.p.value + '> "' + selected_term.o.value + '"@' + selected_term.o['xml:lang'] + ' . ';
												 } else if (selected_term.o['datatype']) {
												 bgp += '?2 <' + selected_term.p.value + '> "' + selected_term.o.value + '"^^<' + selected_term.o['datatype'] + '> . ';
												 } else {
												 bgp += '?2 <' + selected_term.p.value + '> "' + selected_term.o.value + '" . ';
												 }
												 } else {*/

												bgp += '?2 <' + selected_term[0].p.value + '> ?3 .';

												if (selected_term[0].o.type == 'uri') {
													var terms2 = executeQuery('SELECT DISTINCT ?p, ?o WHERE { <' + selected_term[0].o.value + '> ?p ?o . FILTER NOT EXISTS { ?o ?y ?z } }', function(terms2) {

														var selected_term2 = pickRandom(terms2, 1);

														if (selected_term2.length > 0) {

															if (selected_term2[0].o['xml:lang']) {
																bgp += '?3 <' + selected_term2[0].p.value + '> "' + addslashes(selected_term2[0].o.value) + '"@' + selected_term2[0].o['xml:lang'] + ' . ';
															} else if (selected_term.o['datatype']) {
																bgp += '?3 <' + selected_term2[0].p.value + '> "' + addslashes(selected_term2[0].o.value) + '"^^<' + selected_term2[0].o['datatype'] + '> . ';
															} else {
																bgp += '?3 <' + selected_term2[0].p.value + '> "' + addslashes(selected_term2[0].o.value) + '" . ';
															}

															addQuery(bgp);
														}
													});

												} else {
													addQuery(bgp);
												}
											}
										});
									}
								});
							}
						});
					}
				});
			}
		});
	}
}

function addQuery(bgp) {
	var query = 'SELECT * WHERE { ' + bgp + ' }\n';
	console.log('last step');
	executeQuery(query, function(results) {

		console.log('Adding step; results: ' + results.length);

		if (results.length > 0) {

			console.log('Added: ' + query + ' - ' + results.length + ' results')

			fs.appendFile(outputfile, query, function(err) {
				if (err) {
					console.log(err);
				} else {
					count--;

					console.log("Query " + count + " added to file!");
				}
			});
		}
	});

}

/*
 * Fire SPARQL queries, return array of values
 */
function executeQuery(query, callback) {
	var url = endpoint + encodeURIComponent(query);
	//console.log('Executing query: '+ query);

	var options = {
		url : url,
		headers : {
			'Accept' : 'application/sparql-results+json'
		}
	};

	request(options, function(error, response, body) {
		//console.log(body);

		if (error) {
			console.log(query + "could not be parsed!")
		} else {
			try {
				var result = JSON.parse(body);
			} catch(e) {
				console.log(body);
			}
			callback(result.results.bindings);
		}
	});

}

/*
 * Construct a triple
 */

function constructTriple() {

}

/*
 * Picks a number of values randomly form an array
 */
function pickRandom(values, cnt) {
	var selection = [];

	if (values.length < 1) {
		return selection;
	}

	for (var i = 0; i < cnt; i++) {
		var index = Math.floor(Math.random() * values.length);
		var item = values[index];
		values = values.splice(index, 1);
		selection.push(item);
	}
	//console.log('Selected random object: '+ JSON.stringify(values));

	return selection;
}

start();
