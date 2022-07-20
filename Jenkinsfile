@Library(value="kids-first/aws-infra-jenkins-shared-libraries", changelog=false) _
simple_pipeline {
   cron_schedule = "H 9 * * 1-5"
   notifyMe = "<@U01FZG0PDGU>"
   stage_name_1 = "Install requirements"
   stage_name_2 = "Copy secrets file from AWS"
   stage_name_3 = "Update file permissions"
   stage_name_4 = "Source environment variables"
   stage_name_5 = "Run the thing"
   script_1 = "pip3 install -r requirements.txt"
   script_2 = "aws s3 cp s3://d3b-684194535433-us-east-1-service-secrets/d3b-flywheel-routine-missing-classifications/app.secrets ."
   script_3 = "chmod +x ./app.secrets"
   script_4 = "sh ./app.secrets"
   script_5 = "python3 fw_add_missing_file_classifications_dataView.py"
}