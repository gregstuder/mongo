// Tests the dropping and re-adding of a collection

var st = new ShardingTest( name = "multidrop", shards = 1, verbose = 2, mongos = 3, other = { separateConfig : 1 } )

st.stopBalancer()

var mA = st.s0
var mB = st.s1
var mC = st.s2

var coll = mA.getCollection( name + ".coll" )
var collB = mB.getCollection( coll + "" )
var collC = mC.getCollection( coll + "" )

jsTestLog( "Shard and split collection..." )

var admin = mA.getDB( "admin" )
var config = mA.getDB( "config" )

admin.runCommand({ enableSharding : coll.getDB() + "" })
admin.runCommand({ shardCollection : coll + "", key : { _id : 1 } })

var instanceId = config.collections.findOne().instance
printjson( config.collections.findOne() )

for( var i = -100; i < 100; i++ ){
    printjson( admin.runCommand({ split : coll + "", middle : { _id : i } }) )
}

jsTestLog( "Create versioned connection for each mongos..." )

coll.find().itcount()
collB.find().itcount()
collC.find().itcount()

jsTestLog( "Dropping sharded collection..." )
coll.drop()

assert( ! ( "instance" in config.collections.findOne() ) )

jsTestLog( "Recreating collection..." )

admin.runCommand({ shardCollection : coll + "", key : { _id : 1 } })

var newInstanceId = config.collections.findOne().instance
printjson( config.collections.findOne() )

assert( newInstanceId != instanceId )

for( var i = -10; i < 10; i++ ){
    printjson( admin.runCommand({ split : coll + "", middle : { _id : i } }) )
}

jsTestLog( "Retrying connections..." )

coll.insert({ data : "xxx" })
assert.eq( null, coll.getDB().getLastError() )

jsTestLog( "Checking stale connection..." )

assert.eq( 1, collB.find().itcount() )

print( "A" )

collC.insert({ data : "xxx" })

print( "B" )

assert.eq( null, collC.getDB().getLastError() )

print( "C" )

assert.eq( 2, collC.find().itcount() )

print( "D" )

assert.eq( 2, collB.find().itcount() )

jsTestLog( "Done." )

st.stop()


