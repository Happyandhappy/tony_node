var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";

var findActionTraces = function(db, offset){
    console.log("--------------------offset ------------------------");          
    start = new Date().getTime();
    console.log("start");    
    db.collection("action_traces").find(
        {},
        {_id:0, "trx_id" : 1, "createdAt":1, "receipt":1, "act":1}
    )
    .skip(offset)
    .limit(10000)
    .toArray(async (err, result)=>{
        for ( var i = 0 ; i < result.length ; i++){
            console.log(offset + i);
            if (typeof doc.act.data === "object"){
                if (doc.act.data.quantity != undefined || parseFloat(doc.act.data.quantity) > 0){
                    if (doc.act.data.memo === undefined) doc.act.data.memo = null;
                    // console.log('1');
                    var query = { 
                        "Txid"            : doc.trx_id, 
                        "TimeStamp"       : doc.createdAt, 
                        "Actid"           : doc.receipt.act_digest,
                        "from_username"   : doc.act.data.from === undefined ? null : doc.act.data.from ,
                        "to_username"     : doc.act.data.to === undefined ? null : doc.act.data.to
                    };
    
                    // console.log(doc.trx_id);                
                    var res = null;                    
                    try{
                        res = await db.collection("testing_esof_ledger").findOne(query);                        
                    }catch(err){
                        throw err;
                    }

                    if ( res === null){
                        // console.log('2');
                        var smartQuery = {"_id.sc_account":doc.act.account, "_id.sc_action":doc.act.name, "_id.sc_memo":doc.act.data.memo};
                        var smartResult = null;
                        try{
                            smartResult = await db.collection("smart_contract_list_update")
                                .findOne(smartQuery,{ _id : 0, "from_acc_db": 1,"from_acc_cr" : 1, "to_acc_db":1, "to_acc_cr":1 });
                        }catch(err){
                            throw err;
                        }
                        // console.log('3');
    
                        if (smartResult != null){
                            var data = {
                                            "TimeStamp"             : doc.createdAt,
                                            "Txid"                  : doc.trx_id,
                                            "Actid"                 : doc.receipt.act_digest,
                                            "from_username"         : doc.act.data.from,
                                            "from_account_debit"    : smartResult === null ? null: smartResult.from_acc_db,
                                            "from_account_credit"   : smartResult === null ? null:smartResult.from_acc_cr,
                                            "to_username"           : doc.act.data.to,
                                            "to_account_debit"      : smartResult === null ? null:smartResult.to_acc_db,
                                            "to_account_credit"     : smartResult === null ? null:smartResult.to_acc_cr,
                                            "amount"                : doc.act.data.quantity === undefined ? null : doc.act.data.quantity.split(' ')[0],
                                            "currency"              : doc.act.data.quantity === undefined ? '' : doc.act.data.quantity.split(' ')[1],
                                            "valueinusd"            : doc.act.data.quantity,
                                        };                        
                            var date =new Date(data.TimeStamp);
                            date = new Date(date.getFullYear(),date.getMonth(),date.getDate(),0,0,0,0);
                            var priceResult = null;
                            try{
                                priceResult = await db.collection("eos_price_db").findOne({"date": date});
                            }catch(err){
                                throw err;
                            }           
                            // console.log('4');
                            if (priceResult != null){
                                var amount = Number.isNaN(parseFloat(data.amount) * parseFloat(priceResult['price(USD)'])) ? 0 : parseFloat(data.amount) * parseFloat(priceResult['price(USD)']);
                                data.valueinusd = amount;
                                await db.collection("testing_esof_ledger").insertOne(data);
                                console.log('inserted' + (offset + i));                                                          
                            }
                        }                    
                    }
                } 
            }
        }
        console.log(new Date().getTime() - start);        
        findActionTraces(db, offset + 30000);
    })
}

// MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
//     if (err)throw err;    
//     findActionTraces(client.db("eosdb"), 200000000); 
// });

var findActionTraces2 = async function (db) {
    let counter = 0
    let cursor = db.collection('action_traces').find(
        {},
        {_id:0, "trx_id" : 1, "createdAt":1, "receipt":1, "act":1}
        ).limit(200000000).batchSize(3000000);
    for (let doc = await cursor.next(); doc != null; doc = await cursor.next()) {
            if (typeof doc.act.data === "object"){
                if (doc.act.data.quantity != undefined || parseFloat(doc.act.data.quantity) > 0){
                    if (doc.act.data.memo === undefined) doc.act.data.memo = null;
                    // console.log('1');
                    var query = { 
                        "Txid"            : doc.trx_id, 
                        "TimeStamp"       : doc.createdAt, 
                        "Actid"           : doc.receipt.act_digest,
                        "from_username"   : doc.act.data.from === undefined ? null : doc.act.data.from ,
                        "to_username"     : doc.act.data.to === undefined ? null : doc.act.data.to
                    };
    
                    // console.log(doc.trx_id);                
                    var res = null;
                    let cursor2 = null;
                    try{
                        cursor2 = await db.collection("testing_esof_ledger").find(query).limit(200000000).batchSize(3000000);
                        console.log(await cursor2.next())
                    }catch(err){
                        throw err;
                    }

                    if ( cursor2 != null && await cursor2.next() === null){
                        // console.log('2');
                        var smartQuery = {"_id.sc_account":doc.act.account, "_id.sc_action":doc.act.name, "_id.sc_memo":doc.act.data.memo};
                        var smartResult = null;
                        try{
                            smartResult = await db.collection("smart_contract_list_update")
                                .findOne(smartQuery,{ _id : 0, "from_acc_db": 1,"from_acc_cr" : 1, "to_acc_db":1, "to_acc_cr":1 });
                        }catch(err){
                            throw err;
                        }
                        // console.log('3');
    
                        if (smartResult != null){
                            var data = {
                                            "TimeStamp"             : doc.createdAt,
                                            "Txid"                  : doc.trx_id,
                                            "Actid"                 : doc.receipt.act_digest,
                                            "from_username"         : doc.act.data.from,
                                            "from_account_debit"    : smartResult === null ? null: smartResult.from_acc_db,
                                            "from_account_credit"   : smartResult === null ? null:smartResult.from_acc_cr,
                                            "to_username"           : doc.act.data.to,
                                            "to_account_debit"      : smartResult === null ? null:smartResult.to_acc_db,
                                            "to_account_credit"     : smartResult === null ? null:smartResult.to_acc_cr,
                                            "amount"                : doc.act.data.quantity === undefined ? null : doc.act.data.quantity.split(' ')[0],
                                            "currency"              : doc.act.data.quantity === undefined ? '' : doc.act.data.quantity.split(' ')[1],
                                            "valueinusd"            : doc.act.data.quantity,
                                        };                        
                            var date =new Date(data.TimeStamp);
                            date = new Date(date.getFullYear(),date.getMonth(),date.getDate(),0,0,0,0);
                            var priceResult = null;
                            try{
                                priceResult = await db.collection("eos_price_db").findOne({"date": date});
                            }catch(err){
                                throw err;
                            }           
                            // console.log('4');
                            if (priceResult != null){
                                var amount = Number.isNaN(parseFloat(data.amount) * parseFloat(priceResult['price(USD)'])) ? 0 : parseFloat(data.amount) * parseFloat(priceResult['price(USD)']);
                                data.valueinusd = amount;
                                await db.collection("testing_esof_ledger").insertOne(data);
                                console.log('inserted' + counter++);                                                          
                            }
                        }                    
                    }
                } 
            }
    }
}
MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
    if (err)throw err;    
    findActionTraces2(client.db("eosdb")); 
});

