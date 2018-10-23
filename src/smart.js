var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";

var findActionTraces = function(db){    
    db.collection("smart_contract_list_update").find({})  
    .toArray(async (err, result)=>{
        if (err) throw err;
        console.log('------------------offset------------------');
        for ( var i = 0 ; i < result.length ; i++){            
            var data = {
                "_id"           : result[i]._id,
                "To_event1_id"  : 0,
                "To_event2_id"  : 0,
                "From_event1_id": 0,
                "From_event2_id": 0
            }
            console.log(data);
            try{                
                // await db.collection("smart_contract_list").insertOne(data);
                console.log("inserted one")
            }catch(err){}
        }        
    });
}

MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
    if (err)throw err;
    findActionTraces(client.db("eosdb")); 
});
