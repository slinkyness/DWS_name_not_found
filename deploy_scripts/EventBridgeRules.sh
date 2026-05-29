# Variables
REGION=us-east-1
ACCOUNT=533267251991

# helper function for scheduled rules
create_rule() {
    local rule_name=$1
    local schedule=$2
    local function_name=$3

    echo "Creating scheduled rule for $function_name..."

    aws events put-rule \
        --name "$rule_name" \
        --schedule-expression "$schedule" \
        --state ENABLED \
        --region $REGION > /dev/null

    aws lambda add-permission \
        --function-name "$function_name" \
        --statement-id "eventbridge-$rule_name" \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:$REGION:$ACCOUNT:rule/$rule_name" \
        --region $REGION > /dev/null 2>&1

    aws events put-targets \
        --rule "$rule_name" \
        --targets "Id=1,Arn=arn:aws:lambda:$REGION:$ACCOUNT:function:$function_name" \
        --region $REGION > /dev/null

    echo "  Done: $function_name -> $schedule"
}

# helper function for S3 event pattern rules
create_route_rule() {
    local rule_name=$1
    local key_pattern=$2
    local function_name=$3

    echo "Creating route rule for $function_name..."

    aws events put-rule \
        --name "$rule_name" \
        --event-pattern "{\"source\":[\"aws.s3\"],\"detail-type\":[\"Object Created\"],\"detail\":{\"bucket\":{\"name\":[\"REDACTED_S3_BUCKET\"]},\"object\":{\"key\":[{\"wildcard\":\"$key_pattern\"}]}}}" \
        --state ENABLED \
        --region $REGION > /dev/null

    aws lambda add-permission \
        --function-name "$function_name" \
        --statement-id "eventbridge-$rule_name" \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:$REGION:$ACCOUNT:rule/$rule_name" \
        --region $REGION > /dev/null 2>&1

    aws events put-targets \
        --rule "$rule_name" \
        --targets "Id=1,Arn=arn:aws:lambda:$REGION:$ACCOUNT:function:$function_name" \
        --region $REGION > /dev/null

    echo "  Done: $function_name <- s3 key: $key_pattern"
}

create_rule "news_fetch_daily"     "cron(0 6 * * ? *)"    "news_fetch"
create_rule "ct_us_fetch_daily"    "cron(0 6 * * ? *)"    "ct_us_fetch"
create_rule "current_fetch_weekly" "cron(0 6 ? * MON *)"  "current_fetch"
create_rule "nih_fetch_weekly"     "cron(0 6 ? * MON *)"  "nih_fetch"
create_rule "core_fetch_weekly"    "cron(0 6 ? * MON *)"  "core_fetch"

echo ""

create_route_rule "route-who-files"               "raw/*/who_*.json"               "article_process"
create_route_rule "route-news-files"              "raw/*/news_*.json"              "article_process"
create_route_rule "route-currentapi-files"        "raw/*/currentapi_*.json"        "article_process"
create_route_rule "route-clinical_trials_us-files" "raw/*/clinical_trials_us_*.json" "trials_process"
create_route_rule "route-core-files"              "raw/*/core_*.json"              "core_process"
create_route_rule "route-ctis-files"              "raw/*/CTIS_*.csv"               "trials_process"
create_route_rule "route-gho-files"               "raw/*/gho_*.json"               "gho_process"
create_route_rule "route-ihme-files"              "raw/*/IHME*.csv"                "ihme_process"

echo ""
echo "All EventBridge rules have been created!"