var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";

var findActionTraces = function(db, offset){
    console.log('------------------offset------------------');
    db.collection("action_traces").find(
        {},
        {_id:0, "trx_id" : 1, "createdAt":1, "receipt":1, "act":1}        
    )
    .skip(offset)
    .limit(10000)
    .toArray(async (err, result)=>{
        for ( var i = 0 ; i < 10000 ; i++){
            console.log(i + offset);
            var memo = null;
            if (typeof result[i].act.data != "object") memo = null;
            else if (result[i].act.data.memo === undefined) memo = '';
            else memo = result[i].act.data.memo;
            try{                
                await db.collection("smart_contract_list_test").insertOne({
                    "_id":{
                        "sc_account" : result[i].act.account,
                        "sc_action":result[i].act.name, 
                        "sc_memo":memo,    
                    },
                    "is_financial" : '1',
                    "is_social"    :'',
                    "is_politica" : '',
                    "is_marketing" : "",
                    "from_acc_db" : "",
                    "from_acc_cr":"",
                    "to_acc_db" : "",
                    "to_acc_cr" : ""
                });
                console.log("inserted one")
            }catch(err){}
        }
        findActionTraces(db, offset + 10000);
    });
}

MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
    if (err)throw err;
    findActionTraces(client.db("eosdb"), 0); 
});
