var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";
var sourceCollection = "global_eso_ledger";
var smartCcollection = "smart_contract_list_update";
var gEventCollection = "global_events";
var destiCollection = "eos_accounting_ledger";

var addAccountLedgers = async function(db, offset){
    console.log("-----------------start------------------------");
    
    let cursor = db.collection(sourceCollection)
                   .find({},{_id : 0, "Sc_account" : 1, "Sc_name" : 1, "Sc_memo" : 1})
                   .limit(1000).batchSize(100);

    for (let doc = await cursor.next(); doc != null; doc = await cursor.next()){        
        var query = {
            "_id.sc_account"    : doc.Sc_account,
            "_id.sc_action"     : doc.Sc_name,
            "_id.sc_memo"       : doc.Sc_memo
        };
        var smartClist = await db.collection(smartCcollection).findOne(query,{ _id : 0 , "from_event1" : 1, "from_event2" : 1, "to_event1" : 1, "to_event2" : 1});
        if ( smartClist != null){
            var fromEvent1       =   await db.collection(gEventCollection).findOne({"event_id" : 3/*smartClist.from_event1*/});
            var data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : doc.From_username,
                "Account"       : fromEvent1.account_db.acc1_id,
                "Accountchange" : fromEvent1.account_db.acc1_ratio * doc.Amount,
                "Accountbalance": fromEvent1.account_db
            };
            await db.collection(destiCollection).insertOne(data);
            
            var fromEvent2       =   await db.collection(gEventCollection).findOne({"event_id" : smartClist.from_event2});
            data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : doc.From_username,
                "Account"       : fromEvent2.account_db.acc2_id,
                "Accountchange" : fromEvent2.account_db.acc2_ratio * doc.Amount,
                "Accountbalance": fromEvent2.account_db
            }
            await db.collection(destiCollection).insertOne(data);
            
            var toEvent1         =   await db.collection(gEventCollection).findOne({"event_id" : smartClist.to_event1});
            data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : doc.From_username,
                "Account"       : event.account_db.acc2_id,
                "Accountchange" : event.account_db.acc2_ratio * doc.Amount,
                "Accountbalance": event.account_db
            }
            await db.collection(destiCollection).insertOne(data);

            var toEvent2         =   await db.collection(gEventCollection).findOne({"event_id" : smartClist.to_event2});
            data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : doc.From_username,
                "Account"       : toEvent2.account_db.acc2_id,
                "Accountchange" : toEvent2.account_db.acc2_ratio * doc.Amount,
                "Accountbalance": toEvent2.account_db
            }
            await db.collection(destiCollection).insertOne(data);
        }            
    }
}

MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
    if (err)throw err;        
    addAccountLedgers(client.db("eosdb"),0);
});
