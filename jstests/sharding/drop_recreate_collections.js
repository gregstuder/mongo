// Tests dropping and recreating collections, and ensures that mongos is not confused

// Create a new sharded setup
var st = new ShardingTest({ shards : 2, mongos : 2, verbose : 2, other : { separateConfig : true } })

st.stopBalancer()

var mongosA = st.s0
var mongosB = st.s1

var shardDocs = mongosA.getDB( "config" ).shards.find().toArray()
assert.eq( 2, shardDocs.length )

var shardA = new Mongo( shardDocs[0].host )
shardA.shardName = shardDocs[0]._id
    
var shardB = new Mongo( shardDocs[1].host )
shardB.shardName = shardDocs[1]._id 
    
// Function to presplit a collection starting from a base shard, always ends up with the same shard versions
// but chunk location will be opposite if primary shard is opposite
var distributeChunks = function( mongos, coll, primaryShardName, numSplits ){
           
    jsTest.log( "Distributing " + ( numSplits + 1 ) + " chunks for " + coll + " via " + mongos + " starting with " + primaryShardName )
    
    var admin = mongos.getDB( "admin" )
    var config = mongos.getDB( "config" )
    
    var otherShardName = ( primaryShardName == shardA.shardName ? shardB.shardName : shardA.shardName )
    
    // Create the collection if it doesn't exist
    mongos.getCollection( coll + "" ).findOne()
    
    // Set primary shard
    if( config.databases.findOne({ _id : coll.getDB() + "" }).primary != primaryShardName )
        printjson( admin.runCommand({ movePrimary : coll.getDB() + "", to : primaryShardName }) )
    
    // Enable sharding if not already enabled
    if( config.databases.findOne({ _id : coll.getDB() + "", partitioned : true }) == null )
        printjson( admin.runCommand({ enableSharding : coll.getDB() + "" }) )
  
    if( config.collections.findOne({ _id : coll + "", key : { _id : 1 } }) == null )
        printjson( admin.runCommand({ shardCollection : coll + "", key : { _id : 1 } }) )
    
    // Make sure we only have a single chunk to start
    assert.eq( 1, config.chunks.find({ ns : coll + "" }).count() )
    
    for( var i = 0; i < numSplits; i++ )
        printjson( admin.runCommand({ split : coll + "", middle : { _id : i } }) )
  
    // Move half the chunks to the other shard
    for( var i = 0; i < numSplits; i += 2 )
        printjson( admin.runCommand({ moveChunk : coll + "", find : { _id : i }, to : otherShardName }) )
        
    // Move a final chunk back and forth to shardA no matter what - makes sure the final collection and shard 
    // versions are identical (but chunks are actually placed on exactly opposite shards)
    printjson( admin.runCommand({ moveChunk : coll + "", find : { _id : ( primaryShardName == shardA.shardName ? 0 : 1 ) }, to : shardA.shardName }) )
    printjson( admin.runCommand({ moveChunk : coll + "", find : { _id : ( primaryShardName == shardA.shardName ? 0 : 1 ) }, to : shardB.shardName }) )
        
}

// Don't use same db b/c of movePrimary issues
var collFooA = mongosA.getCollection( "foo.foo" )
var collFooB = mongosB.getCollection( "foo.foo" )

// Function to return alternating shards, to break things...
var currShard = shardB
var nextShard = function(){
    currShard = currShard == shardA ? shardB : shardA
    return currShard
}


// SplitDiff :
// -1 means that the recreated collection will have a version less than the previously detected collection,
// 0 means the version will be exactly the same 
// 1 means the version will be greater
for( var splitDiff = -1; splitDiff <= 1; splitDiff++ ){
    
    var splits = 7
    
    collFooA.drop()
    distributeChunks( mongosA, collFooA, nextShard().shardName, ( splits += splitDiff ) )
    
    st.printShardingStatus()
    
    collFooA.findOne()
    collFooB.findOne()
    
    jsTest.log( "Collections up to date on both mongoses..." )
    
    ////
    // Collections are now both up to date and empty
    ////
    
    ////
    // Tests that inserts go to the right place when the collection changes
    ////
    
    jsTest.log( "INSERT: Dropping " + collFooA + " through " + mongosA + " and inverting chunks..." )
    
    collFooA.drop()
    distributeChunks( mongosA, collFooA, nextShard().shardName, ( splits += splitDiff ) )
    
    st.printShardingStatus()
    
    jsTest.log( "INSERT: Chunks for mongosB are now inverse of mongosA..." )
    
    // Inserts into the wrong collection without instance tracking
    collFooB.insert({ _id : -1 })
    assert.eq( null, collFooB.getDB().getLastError() )
    
    // Can't be found if inserted wrong
    assert.neq( null, collFooA.findOne({ _id : -1 }) )
    
    ////
    // Tests that updates go to the right place when the collection changes
    ////
    
    jsTest.log( "UPDATE: Dropping " + collFooA + " through " + mongosA + " and inverting chunks..." )
    
    collFooA.drop()
    distributeChunks( mongosA, collFooA, nextShard().shardName, ( splits += splitDiff ) )
    
    st.printShardingStatus()
    
    jsTest.log( "UPDATE: Chunks for mongosB are now inverse of mongosA..." )
    
    // Inserts into the wrong collection without instance tracking
    collFooB.update({ _id : -1 }, { $set : { data : "hello" } }, true)
    collFooB.update({ _id : -1 }, { $set : { data : "world" } } )
    assert.eq( null, collFooB.getDB().getLastError() )
    
    // Can't be found if updated wrong
    assert.neq( null, collFooA.findOne({ _id : -1 }) )
    assert.eq( "world", collFooA.findOne({ _id : -1 }).data )
    
    ////
    //  Tests that finds go to the right place when the collection changes
    ////
    
    jsTest.log( "FIND: Dropping " + collFooA + " through " + mongosA + " and inverting chunks..." )
    
    collFooA.drop()
    distributeChunks( mongosA, collFooA, nextShard().shardName, ( splits += splitDiff ) )
    
    st.printShardingStatus()
    
    jsTest.log( "FIND: Chunks for mongosB are now inverse of mongosA..." )
    
    // Looks in the wrong collection without instance tracking
    collFooA.insert({ _id : -1 })
    assert.eq( null, collFooA.getDB().getLastError() )
    
    assert.neq( null, collFooB.findOne({ _id : -1 }) )
    
}

st.stop()


