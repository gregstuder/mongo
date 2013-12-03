load("jstests/replsets/rslib.js");

//
// Start a replicaset
var name = "writeCommandNodes";
var host = getHostName();
var port = allocatePorts( 1, 33000 )[ 0 ];

jsTest.log("Starting replset write command tests...");

var replTest = new ReplSetTest( {name: name, nodes: 3, startPort: port} );
var nodes = replTest.startSet();
replTest.initiate();
var master = replTest.getMaster();

// get db and collection, then preform a trivial insert 
var db = master.getDB("test")
var collectionName = "batch_write_protocol";
var coll = db.getCollection(collectionName);

/*******************************************************************
 *
 * Ordered
 *
 *******************************************************************/
var executeOrderedTests = function() {
	// Create unique index
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	//
	// Fail due to write concern support
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({a:1});
	batch.insert({a:2});
	var result = batch.execute({w:5, wtimeout:1});
	assert.eq(2, result.n);
	assert.eq(65, result.getSingleError().code);
	assert(typeof result.getSingleError().errmsg == 'string');
	assert.eq(true, result.hasErrors());
	assert.eq(2, result.getErrorCount());
	assert.eq(2, result.getWCErrors().length);

	// Test errors for expected behavior
	assert.eq(0, result.getErrorAt(0).index);
	assert.eq(64, result.getErrorAt(0).code);
	assert(typeof result.getErrorAt(0).errmsg == 'string');
	assert.eq(1, result.getErrorAt(0).getOperation().a);

	// Create unique index
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	//
	// Fail due to write concern support as well as duplicate key error on ordered batch
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({a:1});
	batch.find({a:3}).upsert().updateOne({a:3, b:1})
	batch.insert({a:1})
	batch.insert({a:2});
	var result = batch.execute({w:5, wtimeout:1});
	assert.eq(2, result.n);
	assert.eq(65, result.getSingleError().code);
	assert(typeof result.getSingleError().errmsg == 'string');
	assert.eq(true, result.hasErrors());
	assert.eq(3, result.getErrorCount());
	assert.eq(2, result.getWCErrors().length);

	// Test errors for expected behavior
	assert.eq(0, result.getErrorAt(0).index);
	assert.eq(64, result.getErrorAt(0).code);
	assert(typeof result.getErrorAt(0).errmsg == 'string');
	assert.eq(1, result.getErrorAt(0).getOperation().a);

	assert.eq(1, result.getErrorAt(1).index);
	assert.eq(64, result.getErrorAt(1).code);
	assert(typeof result.getErrorAt(1).errmsg == 'string');
	assert.eq(3, result.getErrorAt(1).getOperation().q.a);

	assert.eq(2, result.getErrorAt(2).index);
	assert.eq(11000, result.getErrorAt(2).code);
	assert(typeof result.getErrorAt(2).errmsg == 'string');
	assert.eq(1, result.getErrorAt(2).getOperation().a);

	var upserts = result.getUpsertedIds();
	assert.eq(1, upserts.length);
	assert.eq(1, upserts[0].index);
	assert(upserts[0]._id != null);
}

/*******************************************************************
 *
 * Unordered
 *
 *******************************************************************/
var executeUnorderedTests = function() {
	// Create unique index
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	//
	// Fail due to write concern support
	var batch = coll.initializeUnorderedBulkOp();
	batch.insert({a:1});
	batch.find({a:3}).upsert().updateOne({a:3, b:1})
	batch.insert({a:2});
	var result = batch.execute({w:5, wtimeout:1});
	assert.eq(3, result.n);
	assert.eq(65, result.getSingleError().code);
	assert(typeof result.getSingleError().errmsg == 'string');
	assert.eq(true, result.hasErrors());
	assert.eq(3, result.getErrorCount());
	assert.eq(3, result.getWCErrors().length);

	// Test errors for expected behavior
	assert.eq(0, result.getErrorAt(0).index);
	assert.eq(64, result.getErrorAt(0).code);
	assert(typeof result.getErrorAt(0).errmsg == 'string');
	assert.eq(1, result.getErrorAt(0).getOperation().a);

	assert.eq(1, result.getErrorAt(1).index);
	assert.eq(64, result.getErrorAt(1).code);
	assert(typeof result.getErrorAt(1).errmsg == 'string');
	assert.eq(3, result.getErrorAt(1).getOperation().q.a);

	assert.eq(2, result.getErrorAt(2).index);
	assert.eq(64, result.getErrorAt(2).code);
	assert(typeof result.getErrorAt(2).errmsg == 'string');
	assert.eq(2, result.getErrorAt(2).getOperation().a);

	// Create unique index
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	//
	// Fail due to write concern support as well as duplicate key error on unordered batch
	var batch = coll.initializeUnorderedBulkOp();
	batch.insert({a:1});
	batch.find({a:3}).upsert().updateOne({a:3, b:1})
	batch.insert({a:1})
	batch.insert({a:2});
	var result = batch.execute({w:5, wtimeout:1});
	assert.eq(3, result.n);
	assert.eq(65, result.getSingleError().code);
	assert(typeof result.getSingleError().errmsg == 'string');
	assert.eq(true, result.hasErrors());
	assert.eq(4, result.getErrorCount());
	assert.eq(3, result.getWCErrors().length);

	// Test errors for expected behavior
	assert.eq(0, result.getErrorAt(0).index);
	assert.eq(64, result.getErrorAt(0).code);
	assert(typeof result.getErrorAt(0).errmsg == 'string');
	assert.eq(1, result.getErrorAt(0).getOperation().a);

	assert.eq(1, result.getErrorAt(1).index);
	assert.eq(64, result.getErrorAt(1).code);
	assert(typeof result.getErrorAt(1).errmsg == 'string');
	assert.eq(3, result.getErrorAt(1).getOperation().q.a);

	assert.eq(2, result.getErrorAt(2).index);
	assert.eq(11000, result.getErrorAt(2).code);
	assert(typeof result.getErrorAt(2).errmsg == 'string');
	assert.eq(1, result.getErrorAt(2).getOperation().a);

	var upserts = result.getUpsertedIds();
	assert.eq(1, upserts.length);
	assert.eq(1, upserts[0].index);
	assert(upserts[0]._id != null);
}


// Save the existing useWriteCommands function
var _useWriteCommands = coll._mongo.useWriteCommands;

// Force the use of useWriteCommands
coll._mongo.useWriteCommands = function() {
	return true;
}

// Execute tests using legacy operations
executeOrderedTests();
executeUnorderedTests();

// Force the use of legacy commands
coll._mongo.useWriteCommands = function() {
	return false;
}

// Execute tests using legacy operations
executeOrderedTests();
executeUnorderedTests();

// Reset the function
coll._mongo.useWriteCommands = _useWriteCommands;