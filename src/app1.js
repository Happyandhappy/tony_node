var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";

var findActionTraces = function(db, offset){
    console.log("--------------------offset ------------------------");
    console.log(offset);      
    start = new Date().getMilliseconds();
    db.collection("action_traces").find(
        {},
        {_id:0, "trx_id" : 1, "createdAt":1, "receipt":1, "act":1}
    )
    .skip(offset)
    .limit(10000)
    .toArray(async (err, result)=>{
        for ( var i = 0 ; i < result.length ; i++){
            // console.log(offset + i);
            if (typeof result[i].act.data === "object"){            	
                if (result[i].act.data.quantity != undefined || parseFloat(result[i].act.data.quantity) > 0){
                    if (result[i].act.data.memo === undefined) result[i].act.data.memo = null;
                    // console.log('1');                    
                    var query = { 
                        "Txid"            : result[i].trx_id, 
                        "TimeStamp"       : result[i].createdAt, 
                        "Actid"           : result[i].receipt.act_digest,
                        "from_username"   : result[i].act.data.from === undefined ? null : result[i].act.data.from ,
                        "to_username"     : result[i].act.data.to === undefined ? null : result[i].act.data.to
                    };
    
                    // console.log(result[i].trx_id);                
                    var res = null;                    
                    try{
                        res = await db.collection("testing_esof_ledger").findOne(query);                        
                    }catch(err){
                        throw err;
                    }
					
                    if ( res === null){
                        // console.log('2');                        
                        var smartQuery = {"_id.sc_account":result[i].act.account, "_id.sc_action":result[i].act.name, "_id.sc_memo":result[i].act.data.memo};
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
                                            "TimeStamp"             : result[i].createdAt,
                                            "Txid"                  : result[i].trx_id,
                                            "Actid"                 : result[i].receipt.act_digest,
                                            "from_username"         : result[i].act.data.from,
                                            "from_account_debit"    : smartResult === null ? null: smartResult.from_acc_db,
                                            "from_account_credit"   : smartResult === null ? null:smartResult.from_acc_cr,
                                            "to_username"           : result[i].act.data.to,
                                            "to_account_debit"      : smartResult === null ? null:smartResult.to_acc_db,
                                            "to_account_credit"     : smartResult === null ? null:smartResult.to_acc_cr,
                                            "amount"                : result[i].act.data.quantity === undefined ? null : result[i].act.data.quantity.split(' ')[0],
                                            "currency"              : result[i].act.data.quantity === undefined ? '' : result[i].act.data.quantity.split(' ')[1],
                                            "valueinusd"            : result[i].act.data.quantity,
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
                            	console.log(offset + i);
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
        findActionTraces(db, offset + 30000);
    })
}

MongoClient.connect( url , { useNewUrlParser: true },  function(err, client){
    if (err)throw err;    
    findActionTraces(client.db("eosdb"), 10000); 
});
