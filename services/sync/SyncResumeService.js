function getResumePoint(batch){

    if(!batch)
        return "START";


    if(
        batch.current_module==="GROUP" &&
        batch.current_action==="COMPLETED"
    ){
        return "STOCK_GROUP";
    }


    if(
        batch.current_module==="STOCK_GROUP" &&
        batch.current_action==="COMPLETED"
    ){
        return "LEDGER";
    }


    if(
        batch.current_module==="LEDGER" &&
        batch.current_action==="COMPLETED"
    ){
        return "GODOWN";
    }


    if(
        batch.current_module==="GODOWN" &&
        batch.current_action==="COMPLETED"
    ){
        return "UNIT";
    }


    if(
        batch.current_module==="UNIT" &&
        batch.current_action==="COMPLETED"
    ){
        return "COST_CENTRE";
    }


    if(
        batch.current_module==="COST_CENTRE" &&
        batch.current_action==="COMPLETED"
    ){
        return "STOCK";
    }


    if(
        batch.current_module==="STOCK" &&
        batch.current_action==="COMPLETED"
    ){
        return "VOUCHER";
    }


    return batch.current_module;

}


module.exports={
    getResumePoint
};