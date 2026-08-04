async function runJob(job){

    switch(job.job_type){


        case "TALLY_SYNC":

            console.log(
                "Running TALLY_SYNC",
                job.batch_id
            );

            // next step me tally sync call hoga

            break;



        case "INVOICE_GENERATE":

            console.log(
                "Running INVOICE_GENERATE",
                job.batch_id
            );

            break;



        case "REPORT_GENERATE":

            console.log(
                "Running REPORT_GENERATE",
                job.batch_id
            );

            break;



        default:

            throw new Error(
                "Unknown job type: "
                + job.job_type
            );

    }

}


module.exports = {
    runJob
};