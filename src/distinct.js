var MongoClient = require('mongodb').MongoClient;
var start = 0, cnt = 0;
var url = "mongodb://admin:LanaRhoades456@172.104.83.46:27017/eosdb";


MongoClient.connect( url , { useNewUrlParser: true }, async function(err, client){
	var db = client.db("eosdb");
    if (err)throw err;
    var res = await db.collection("action_traces").distinct("act.account",{});

    for ( var i = 0 ; i < res.length ; i++){
    	res1 = []
    	try{
    		var res1 = await db.collection("action_traces").distinct("act.name", {"act.account" : res[i]});
    	}catch(err){}

    	for ( var j = 0; j < res1.length ; j++){
    		res2 = []
    		try{
    			var res2 = await db.collection("action_traces").distinct("act.data.memo", {"act.account" : res[i], "act.name" : res1[j]});
    		}catch(err){}

    		if (res2.length === 0){   
    			try{ 			
	                await db.collection("smart_contract_list_test").insertOne({
	                    "_id":{
	                        "sc_account" : res[i],
	                        "sc_action":res1[j], 
	                        "sc_memo":'',
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
	                console.log("inserted");
	            }catch(err){}
            }
            else{
            	try{
	            	for ( var k = 0 ; k < res2.length ; k++){
	            		await db.collection("smart_contract_list_test").insertOne({
		                    "_id":{
		                        "sc_account" 	: res[i],
		                        "sc_action"		:res1[j], 
		                        "sc_memo"		:res2[k],
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
		                console.log("inserted");
	            	}
	            }catch(err){}
            }
    	}
    }
    // var res = client.db("eosdb").collection("action_traces").distinct("act.name",{"act.account":"eosio"}, function(err, doc){
    // 	console.log(doc);
    // });

    // var res = client.db("eosdb").collection("action_traces").distinct("act.data.memo",{"act.account":"eosio", "act.name":"undelegatebw"}, function(err, doc){
    // 	if (err) throw err
    // 	console.log(doc);
    // });
    // , "act.name", "act.data.memo"
    // findActionTraces(client.db("eosdb"));
});