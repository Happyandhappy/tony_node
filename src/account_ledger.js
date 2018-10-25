var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://tony:Harv3y456@127.0.0.1:27017";
var sourceCollection = "global_eso_ledger";
var smartCcollection = "smart_contracts_eos";
var gEventCollection = "global_events";
var destiCollection = "eos_accounting_ledger";
var refCollection   = "eos_accounting_ledger_ref";

var insert = async function(db, doc, event, isFrom){
        var Username = isFrom ? doc.From_username : doc.To_username;
        if (event.account_db.acc1_id != null){
            var query = { "Username" : Username, "Account" : event.account_db.acc1_id};
            var lastRecord = await db.collection(refCollection).findOne(query);            
            var data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : Username,
                "Account"       : event.account_db.acc1_id,
                "Accountchange" : event.account_db.acc1_ratio * doc.Amount,
                "Accountbalance": lastRecord === null ? event.account_db.acc1_ratio * doc.Amount : lastRecord.Accountbalance + event.account_db.acc1_ratio * doc.Amount
            };            
            await db.collection(destiCollection).insertOne(data);            
            if (lastRecord === null){
                await db.collection(refCollection).insertOne(data);
            }else{
                var newValue ={ $set: {
                    "TimeStamp" : doc.TimeStamp,
                    "Accountchange" : data.Accountchange,
                    "Accountbalance" : data.Accountbalance
                }};
                await db.collection(refCollection).updateOne(query, newValue);                
            }
        }
        
        if (event.account_db.acc2_id != null){
            var query = { "Username" : Username, "Account" : event.account_db.acc2_id};
            lastRecord       =   await db.collection(refCollection).findOne(query);
            data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : Username,
                "Account"       : event.account_db.acc2_id,
                "Accountchange" : event.account_db.acc2_ratio * doc.Amount,
                "Accountbalance": lastRecord === null ? event.account_db.acc2_ratio * doc.Amount : lastRecord.Accountbalance + event.account_db.acc2_ratio * doc.Amount
            }
            
            await db.collection(destiCollection).insertOne(data);
            if (lastRecord === null){
                await db.collection(refCollection).insertOne(data);
            }else{
                var newValue ={ $set: {
                    "TimeStamp" : doc.TimeStamp,
                    "Accountchange" : data.Accountchange,
                    "Accountbalance" : data.Accountbalance
                }};
                await db.collection(refCollection).updateOne(query, newValue); 
            }
        }


        if (event.account_cr.acc1_id != null){
            var query = { "Username" : Username, "Account" : event.account_cr.acc1_id}
            lastRecord       =   await db.collection(refCollection).findOne(query);
            data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : Username,
                "Account"       : event.account_cr.acc1_id,
                "Accountchange" : -event.account_cr.acc1_ratio * doc.Amount,
                "Accountbalance": lastRecord === null ? -event.account_cr.acc1_ratio * doc.Amount : lastRecord.Accountbalance - event.account_cr.acc1_ratio * doc.Amount
            }

            await db.collection(destiCollection).insertOne(data);
            if (lastRecord === null){
                await db.collection(refCollection).insertOne(data);
            }else{
                var newValue ={ $set: {
                    "TimeStamp" : doc.TimeStamp,
                    "Accountchange" : data.Accountchange,
                    "Accountbalance" : data.Accountbalance
                }};
                await db.collection(refCollection).updateOne(query, newValue);
            }
        }

        if (event.account_cr.acc2_id != null){
            var query = { "Username" : Username, "Account" : event.account_cr.acc2_id};
            lastRecord       =   await db.collection(refCollection).findOne(query);
            data = {
                "TimeStamp"     : doc.TimeStamp,
                "Username"      : Username,
                "Account"       : event.account_cr.acc2_id,
                "Accountchange" : -event.account_cr.acc2_ratio * doc.Amount,
                "Accountbalance": lastRecord === null ? -event.account_cr.acc2_ratio * doc.Amount : lastRecord.Accountbalance - event.account_cr.acc2_ratio * doc.Amount
            }

            await db.collection(destiCollection).insertOne(data);
            if (lastRecord === null){
                await db.collection(refCollection).insertOne(data);
            }else{
                var newValue ={ $set: {
                    "TimeStamp" : doc.TimeStamp,
                    "Accountchange" : data.Accountchange,
                    "Accountbalance" : data.Accountbalance
                }};
                await db.collection(refCollection).updateOne(query, newValue);
            }
        }
}


var addAccountLedgers = async function(db, offset){
    console.log("-----------------start------------------------");
    
    let cursor = db.collection(sourceCollection)
                   .find({},{_id : 0, "Sc_account" : 1, "Sc_name" : 1, "Sc_memo" : 1})
                   .limit(1).batchSize(1);

    for (let doc = await cursor.next(); doc != null; doc = await cursor.next()){        
        var query = {
            "_id.sc_account"    : doc.Sc_account,
            "_id.sc_action"     : doc.Sc_name,
            "_id.sc_memo"       : doc.Sc_memo
        };        
        var smartClist = await db.collection(smartCcollection).findOne(query,{ _id : 0 , "from_event1" : 1, "from_event2" : 1, "to_event1" : 1, "to_event2" : 1});        
        if ( smartClist != null){            
            if (smartClist.from_event1 != null && smartClist.from_event1 != ''){
                var fromEvent1       =   await db.collection(gEventCollection).findOne({"event_id" : 3/*smartClist.from_event1*/});
                await insert(db,doc,fromEvent1);
            }
            if (smartClist.from_event2 != null && smartClist.from_event2 != ''){
                var fromEvent2       =   await db.collection(gEventCollection).findOne({"event_id" : smartClist.from_event2});
                await insert(db,doc,fromEvent2);
            }
            if (smartClist.to_event1 != null && smartClist.to_event1 != ''){
                var toEvent1         =   await db.collection(gEventCollection).findOne({"event_id" : smartClist.to_event1});
                await insert(db,doc,toEvent1);
            }
            if (smartClist.to_event2 != null && smartClist.to_event2 != ''){
                var toEvent2         =   await db.collection(gEventCollection).findOne({"event_id" : smartClist.to_event2});
                await insert(db, doc, toEvent2);
            }
        }else{            
            var data = {
                _id : {
                    "sc_account" : doc.Sc_account,
                    "sc_action"  : doc.Sc_name,
                    "sc_memo"    : doc.Sc_memo
                },
                "from_event1"    : "",
                "from_event2"    : "",
                "to_event1"      : "",
                "to_event2"      : ""
            };
            try{await db.collection(smartCcollection).insertOne(data);}catch(err){}            
        }
    }
}

MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
    if (err)throw err;
    addAccountLedgers(client.db("EOS"),0);
});
