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
var count = 100;
var outputfile = '/home/mvdrsand/ldf/benchmarks/queries/output.log';
var method = 'SELECT';

function start() {
	console.log('Query generator started!');

	step1();
}

function step1() {
	// 1. SELECT ALL TYPES FROM TRIPLE STORE AND LOOP OVER THEM
	console.log('Entering step 1');
	constructTriples([{
		subject : '?s',
		predicate : 'a',
		object : '?o'
	}], function(triples) {
		console.log('step1');
		setInterval(function() {
			step2(triples)
		}, 1000);
	});
}

function step2(types) {
	// 2. PICK A RANDOM TYPE AND SELECT A RANDOM SUBJECT OF THAT TYPE
	console.log('Entering step 2');
	pickRandom(types, 1, function(selected) {
		if (selected.length > 0) {
			var selected_t = selected[0];
			//console.log("Type: "+ JSON.stringify(selected_t));
			//Add to bgp
			var bgp = toBGPString('?1', 'a', selected_t.object);

			constructTriples([{
				subject : '?s',
				predicate : 'a',
				object : selected_t.object
			}], function(subjects) {
				pickRandom(subjects, 1, function(selected) {
					if (selected.length > 0) {
						var selected_s = selected[0];
						step3(selected_s, bgp);
					}
				});
			})
		}
	});
}

function step3(selected_s, bgp) {
	// 3. SELECT A RANDOM PREDICATE OF THAT SUBJECT
	console.log('Entering step 3');
	constructTriples([{
		subject : selected_s.subject,
		predicate : '?p',
		object : '?o'
	}], function(predicates) {
		pickRandom(predicates, 1, function(selected) {
			if (selected.length > 0) {
				var selected_p = selected[0];
				//Add to bgp
				bgp += toBGPString('?1', selected_p.predicate, '?2');

				step4(selected_s, selected_p, bgp);
			}
		});
	});
}

function step4(selected_s, selected_p, bgp) {
	//4.  SELECT OBJECTS FROM SUBJECT AND PREDICATE
	console.log('Entering step 4');
	constructTriples([{
		subject : selected_s.subject,
		predicate : selected_p.predicate,
		object : '?o'
	}], function(objects) {
		if (objects.length > 1) {
			pickRandom(objects, 2, function(selected) {
				if (selected.length > 0) {
					step5(selected, bgp);
				}
			});
		}
	});
}

function step5(selected, bgp) {
	//5. GET ALL TRIPLES FOR THE SELECTED OBJECT AS SUBJECT AND PICK A RANDOM ONE
	console.log('Entering step 5');
	var counter = 0;
	var finished = [false, false]; //I know it is ugly, but turning everything into async would take too long.
	
	selected.forEach(function(selected_o) {
		if (n3.Util.isUri(selected_o.object)) {
			constructTriples([{
				subject : selected_o.object,
				predicate : '?p',
				object : '?o'
			}], function(terms) {
				pickRandom(terms, 1, function(selected) {
					if (selected.length > 0) {
						var selected_term = selected[0];

						if (n3.Util.isUri(selected_term.object)) {
							var index = counter++;
							bgp += toBGPString('?2', selected_term.predicate, '?' + (index + 3));

							var query = 'CONSTRUCT { <' + selected_term.object + '> ?p ?o } WHERE { <' + selected_term.object + '> ?p ?o . FILTER NOT EXISTS { ?o ?y ?z } }';
							//'custom' query
							executeConstructQuery(query, function(triples) {
								pickRandom(triples, 1, function(selected) {
									if (selected.length > 0) {
										var selected_term2 = selected[0];
										bgp += toBGPString('?' + (index + 3), selected_term2.predicate, selected_term2.object);
										finished[index] = true;
										console.log(JSON.stringify(finished));
										if (finished[0] && finished[1])
											addQuery(bgp);
									}
								});
							})
						}
					}
				});
			});

		}
	});
}

function addQuery(bgp) {
	console.log('Entering Query adding step');
	var query = 'SELECT * WHERE { ' + bgp + ' }\n';
	executeSelectQuery(query, function(results) {

		console.log('Adding step; results: ' + results.length);

		if (results.length > 0) {

			console.log('Added: ' + query + ' - ' + results.length + ' results')

			fs.appendFile(outputfile, query, function(err) {
				if (err) {
					console.log(err);
				} else {
					count--;
					console.log("Query " + count + " added to file!");
					if (count <= 0) {
						console.log('Query generation done!');
						process.exit();
					}

				}
			});
		}
	});
}

function executeSelectQuery(query, callback) {
	executeQuery(query, 'application/sparql-results+json', function(body) {
		var result = JSON.parse(body);
		//console.log(query + ' - ' + result.results.bindings.length + ' results');
		callback(result.results.bindings)

	});
}

/*
 * Fire SPARQL queries, return array of triples
 */
function executeConstructQuery(query, callback) {
	executeQuery(query, 'text/plain', function(body) {
		var triples = [];

		parseN3(body, function(triple) {
			triples.push(triple);
		}, function() {
			console.log(query + ' - ' + triples.length + ' results');
			callback(triples)
		}, function(err) {
			console.log(query);
			console.log('Construct query error: ' + err);
		});
	});
}

function executeQuery(query, mediaType, success) {
	var url = endpoint + encodeURIComponent(query);
	//console.log('Executing query: ' + query);
	var options = {
		url : url,
		headers : {
			'Accept' : mediaType
		}
	};

	request(options, function(error, response, body) {
		if (error) {
			console.log(query + "could not be parsed!")
		else if (response.statusCode != 200) {
			console.log(JSON.stringify(response));
		} else {
			success(body);
		}
	});
}

function parseN3(triples, hit, end, error) {
	var parser = new n3.Parser();
	parser.parse(triples, function(err, triple) {
		if (err) {
			error(err);
		} else {
			if (triple) {
				hit(triple);
			} else {
				end();
			}
		}
	});
}

/*
 * Construct a triple
 * desiredTriple {s: }
 *
 */
function constructTriples(bgps, callback) {
	switch(method) {
		case 'SELECT': {
			constructTriplesWithSelect(bgps, callback);
			break;
		}
		case 'CONSTRUCT': {
			constructTriplesWithConstruct(bgps, callback);
		}
	}
}

function constructTriplesWithConstruct(bgps, callback) {
	process.nextTick(function() {
		constructQuery(bgps, function(query) {
			executeConstructQuery(query, function(triples) {
				callback(triples);
			});
		});
	});
}

function constructTriplesWithSelect(bgps, callback) {
	process.nextTick(function() {
		constructSelectQuery(bgps, function(query) {
			executeSelectQuery(query, function(results) {
				var triples = [];
				results.forEach(function(result) {
					var triple = {
						subject : null,
						predicate : null,
						object : null
					};
					if (result.s)
						triple.subject = result.s.value;
					if (result.p)
						triple.predicate = result.p.value;
					if (result.o)
						triple.object = resolveObject(result.o);

					triples.push(triple);
				});
				callback(triples);
			});
		});
	});
}

function resolveObject(object) {
	if (object.type === 'uri')
		return object.value;

	if (object['xml:lang']) {
		return '"' + addslashes(object.value) + '"@' + object['xml:lang'];
	} else if (object['datatype']) {
		return '"' + addslashes(object.value) + '"^^<' + object['datatype'] + '>';
	}
	return '"' + addslashes(object.value) + '"';
}

function wrap(value) {
	function isWildcard(v) {
		return v.indexOf('?') === 0;
	}

	if (value !== 'a' && !isWildcard(value) && n3.Util.isUri(value)) {
		return '<' + value + '>';
	}

	return value;
}

function addslashes(str) {
	return str.replace(/"/g, '\\"');
}

function toBGPString(bgp, p, o) {
	if (p && o)
		return [wrap(bgp), wrap(p), wrap(o)].join(' ') + '. ';

	return [wrap(bgp.subject), wrap(bgp.predicate), wrap(bgp.object)].join(' ') + '. ';
}

function constructQuery(bgps, callback) {

	var bgpString = "";
	bgps.forEach(function(bgp) {
		bgpString += toBGPString(bgp);
	});

	var query = 'CONSTRUCT { ' + bgpString + ' } WHERE { ' + bgpString + '}';

	callback(query);
}

function constructSelectQuery(bgps, callback) {

	var bgpString = "";
	bgps.forEach(function(bgp) {
		bgpString += toBGPString(bgp);
	});

	var query = 'SELECT DISTINCT * WHERE { ' + bgpString + '}';

	callback(query);
}

/*
 * Picks a number of values randomly form an array
 */
function pickRandom(values, cnt, callback) {
	var selection = [];

	if (cnt <= values.length) {
		for (var i = 0; i < cnt; i++) {
			var index = Math.floor(Math.random() * values.length);
			var item = values[index];
			values = values.splice(index, 1);
			selection.push(item);
		}
		//console.log('Selected random object: ' + JSON.stringify(selection));
	} else {
		console.log('Unable to select ' + cnt + ' values out of ' + values.length)
	}
	callback(selection);
}

start();
