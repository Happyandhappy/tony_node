var MongoClient = require('mongodb').MongoClient;
var start = 0, cnt = 0;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";

var findActionTraces = function(db){    
    var collection = db.collection("action_traces");
    start = new Date().getMilliseconds();
    collection.find(
        {},
        {_id:0, "trx_id" : 1, "createdAt":1, "receipt":1, "act":1}
    ).forEach(function(doc){           
        if (typeof doc.act.data === "object"){
            if (doc.act.data.memo === undefined) doc.act.data.memo = null;                        
            findSmartContractList(db, doc);                 
        }
    }, function(err) {        
        console.log(err);
    });    
}

var findSmartContractList = function(db, result){
    var collection = db.collection("smart_contract_list_update");   
    var query = {"_id.sc_account":result.act.account, "_id.sc_action":result.act.name, "_id.sc_memo":result.act.data.memo};    
    collection.findOne(
        query, 
        { _id : 0, "from_acc_db": 1,"from_acc_cr" : 1, "to_acc_db":1, "to_acc_cr":1 },        
        function(err, smartResult){        
            if (err) throw err;                                                                                     
            var data = {
                "TimeStamp"             : result.createdAt,
                "Txid"                  : result.trx_id,
                "Actid"                 : result.receipt.act_digest,
                "from_username"         : result.act.data.from,
                "from_account_debit"    : smartResult === null ? null: smartResult.from_acc_db,
                "from_account_credit"   : smartResult === null ? null:smartResult.from_acc_cr,
                "to_username"           : result.act.data.to,
                "to_account_debit"      : smartResult === null ? null:smartResult.to_acc_db,
                "to_account_credit"     : smartResult === null ? null:smartResult.to_acc_cr,
                "amount"                : result.act.data.quantity === undefined ? null : result.act.data.quantity.split(' ')[0],
                "currency"              : result.act.data.quantity === undefined ? null : result.act.data.quantity.split(' ')[1],
                "valueinusd"            : result.act.data.quantity,            
            };                
            findEOSprices(db, data); 
    }); 
}


var findEOSprices = function(db, data){    
    var collection = db.collection("eos_price_db");
    var date =new Date(data.TimeStamp);    
    date = new Date(date.getFullYear(),date.getMonth(),date.getDate(),2,0,0,0);        

    collection.
        findOne(
            {"date": date},
            function(err, result){            
                if (err) throw err;
                data.valueinusd = result['price(USD)'];
                db.collection("testing_Global_ledger_00").insertOne(data, function(err,res){
                    if (err) throw err;                   
                    if (cnt == 100000) {
                        console.log('---------------------------------------------------------------------');
                        console.log(new Date().getMilliseconds() - start);
                    } else{
                        console.log(cnt++);
                    }
                });
            }
    ); 
}

MongoClient.connect( url , { useNewUrlParser: true }, function(err, client){
    if (err)throw err;    
    findActionTraces(client.db("eosdb"));
});





