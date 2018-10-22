var mongoose = require('mongoose');

var modelSchema = mongoose.Schema({
    _id : mongoose.Schema.Types.ObjectId,
    Timestamp           : { type : String, default : null },
    txid                : { type : String, default : null },
    Actid               : { type : String, default : null },
    from_username       : { type : String, default : null },
    from_account_debit  : { type : String, default : null },
    from_account_credit : { type : String, default : null },
    to_username         : { type : String, default : null },
    to_account_debit    : { type : String, default : null },
    to_account_credit   : { type : String, default : null },
    amount              : { type : String, default : null },
    currency            : { type : String, default : null },
    valueinusd          : { type : String, default : null }
});

var Model = mongoose.model('global_eosf_ledger_new', modelSchema);

module.exports = Model;