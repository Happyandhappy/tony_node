var MongoClient = require('mongodb').MongoClient;
var db;
var start = 0;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";

var findActionTraces = function(db, callback){
    start = new Date().getMilliseconds();
    var collection = db.collection("action_traces");

    collection.aggregate([
        { $lookup:
            {
              from: 'smart_contact_list_update',
              localField: 'act.account',
              foreignField: 'from_acc_db',
              as: 'smart_contract'
            }
        }
    ])
    .limit(1)
    .toArray(function(err, res) {
        if (err) throw err;
        console.log(res);
      });
      
    // .forEach(function(doc){
    //     console.log(doc);
    // });
}

var findSmartContractList = function(db, result){
    var collection = db.collection("smart_contract_list");   
    var query = {"_id.sc_account":result.act.account, "_id.sc_action":result.act.name, "_id.sc_memo":result.act.data.memo};     

    collection.findOne(
        query, 
        { _id : 0, "from_acc_db": 1,"from_acc_cr" : 1, "to_acc_db":1, "to_acc_cr":1 },        
        function(err, smartResult){        
            if (err) throw err;                            
            if (smartResult == null) return;
            var data = {
                "TimeStamp"             : result.createdAt,
                "Txid"                  : result.trx_id,
                "Actid"                 : result.receipt.act_digest,
                "from_username"         : result.act.data.from,
                "from_account_debit"    : smartResult.from_acc_db,
                "from_account_credit"   : smartResult.from_acc_cr,
                "to_username"           : result.act.data.to,
                "to_account_debit"      : smartResult.to_acc_db,
                "to_account_credit"     : smartResult.to_acc_cr,
                "amount"                : result.act.data.quantity,
                "Currency"              : result.act.data.quantity,
                "valueinusd"            : result.act.data.quantity,            
            };
            console.log(data);
            console.log(start++);
            // findEOSprices(db, data); 
    }); 
}


var findEOSprices = function(db, data){
    console.log(data);
    var collection = db.collection("eos_price_db");
    collection.
        findOne({},
        function(err, result){            
            if (err) throw err;
            data.valueinusd = result['price(USD)'];
            console.log(data);
    }); 
}

MongoClient.connect(url, function(err, client){
    if (err)throw err;    
    findActionTraces(client.db("eosdb"), function(){
        db.close();
    });
});




