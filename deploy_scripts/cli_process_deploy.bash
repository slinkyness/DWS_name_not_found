#!/bin/bash

# =============================================================================
# Lambda deployment script
# Usage:
#   bash cli_deployment fetch
#   bash cli_deployment process
#   bash cli_deployment rds
#   bash cli_deployment orchestrator
# =============================================================================


# -- Shared config -------------------------------------------------------------
_ROLE='arn:aws:iam::533267251991:role/LabRole'
_REGION=us-east-1
_S3_bucket=REDACTED_S3_BUCKET

# -- Upsert function -----------------------------------------------------------
# Args: func_name s3_bucket s3_key layer_arns env_vars timeout memory_mb description
# layer_arns is a space-separated list (quoted), e.g. "arn1 arn2 arn3"
_upsert_function() {
    local func_name="$1"
    local s3_bucket="$2"
    local s3_key="$3"
    local layer_arns="$4"   # space-separated, empty string = no layers
    local env_vars="$5"
    local timeout="$6"
    local memory_mb="$7"
    local description="$8"

    # Build --layers flag only when layers are provided
    local layers_flag=()
    if [ -n "$layer_arns" ]; then
        # shellcheck disable=SC2206
        layers_flag=(--layers $layer_arns)
    fi

    echo "--------------------------------------------"
    echo "Processing $func_name  (s3://$s3_bucket/$s3_key)..."

    if AWS_PAGER="" aws lambda get-function \
         --function-name "$func_name" \
         --region "$_REGION" > /dev/null 2>&1; then

        echo "  Function exists — updating configuration..."
        AWS_PAGER="" aws lambda update-function-configuration \
          --function-name  "$func_name" \
          --runtime        python3.11 \
          --role           "$_ROLE" \
          "${layers_flag[@]}" \
          --environment    "$env_vars" \
          --timeout        "$timeout" \
          --memory-size    "$memory_mb" \
          --description    "$description" \
          --region         "$_REGION" > /dev/null

        if [ $? -ne 0 ]; then
            echo "  ❌ Failed to update configuration for $func_name. Stopping."
            return 1
        fi

        echo "  Waiting for configuration update to complete..."
        AWS_PAGER="" aws lambda wait function-updated \
          --function-name "$func_name" --region "$_REGION"

        echo "  Updating code..."
        AWS_PAGER="" aws lambda update-function-code \
          --function-name "$func_name" \
          --s3-bucket     "$s3_bucket" \
          --s3-key        "$s3_key" \
          --architectures arm64 \
          --region        "$_REGION" > /dev/null

        if [ $? -ne 0 ]; then
            echo "  ❌ Failed to update code for $func_name. Stopping."
            return 1
        fi

        echo "  Waiting for code update to complete..."
        AWS_PAGER="" aws lambda wait function-updated \
          --function-name "$func_name" --region "$_REGION"

        echo "  ✅ Updated $func_name."

    else
        echo "  Function not found — creating..."
        AWS_PAGER="" aws lambda create-function \
          --function-name  "$func_name" \
          --runtime        python3.11 \
          --role           "$_ROLE" \
          --handler        lambda_function.lambda_handler \
          --code "S3Bucket=$s3_bucket,S3Key=$s3_key" \
          "${layers_flag[@]}" \
          --environment    "$env_vars" \
          --timeout        "$timeout" \
          --memory-size    "$memory_mb" \
          --description    "$description" \
          --architectures  arm64 \
          --region         "$_REGION" > /dev/null

        if [ $? -ne 0 ]; then
            echo "  ❌ Failed to create $func_name. Stopping."
            return 1
        fi

        echo "  ✅ Created $func_name."
    fi
}

# -- Publish a single layer and return its ARN ---------------------------------
# Args: layer_name s3_prefix description
_publish_layer() {
    local layer_name="$1"
    local s3_prefix="$2"
    local description="$3"

    echo "Publishing layer: $layer_name ..."
    local layer_output
    layer_output=$(AWS_PAGER="" aws lambda publish-layer-version \
      --layer-name "$layer_name" \
      --description "$description" \
      --compatible-runtimes python3.11 \
      --compatible-architectures arm64 \
      --content "S3Bucket=$_S3_bucket,S3Key=$s3_prefix/lambda-layer.zip" \
      --region "$_REGION")

    local arn
    arn=$(echo "$layer_output" | jq -r '.LayerVersionArn')
    if [ -z "$arn" ]; then
        echo "❌ Error: Layer ARN not found for $layer_name."
        return 1
    fi
    echo "✅ Layer published: $arn"
    echo "$arn"   # caller captures this line
}

deploy() {
    local deploy_type="${1:-}"

    case "$deploy_type" in

    # -------------------------------------------------------------------------
    fetch)
        local s3_prefix=scripts/fetch
        local env_vars='Variables={SECRET_NAME=prod/App/fetch,AWS_REGION_NAME=us-east-1,S3_BUCKET=REDACTED_S3_BUCKET,S3_FETCH_FOLDER=raw}'
        local timeout=180
        local memory_mb=256

        # Publish the single shared layer for all fetch functions
        local fetch_layer
        fetch_layer=$(_publish_layer \
            "python311-fetch-layer" \
            "$s3_prefix" \
            "Dependencies for fetch functions") || return 1
        echo ""

        # func_name → "s3_key|layer_arn1 layer_arn2 ..."
        declare -A functions=(
            ["who_fetch"]="who.zip|$fetch_layer"
            ["gho_fetch"]="gho.zip|$fetch_layer"
            ["news_fetch"]="news.zip|$fetch_layer"
            ["current_fetch"]="currents.zip|$fetch_layer"
            ["ct_us_fetch"]="ct_us.zip|$fetch_layer"
            ["nih_fetch"]="nih.zip|$fetch_layer"
            ["core_fetch"]="core.zip|$fetch_layer"
        )
        declare -A descriptions=(
            ["who_fetch"]="Fetches global health statistics from the WHO API"
            ["gho_fetch"]="Fetches indicator data from the GHO (Global Health Observatory) API"
            ["news_fetch"]="Fetches top-headline news articles from a news aggregation API"
            ["current_fetch"]="Fetches current/real-time health event data from the Currents API"
            ["ct_us_fetch"]="Fetches clinical trial data for the US (ClinicalTrials.gov)"
            ["nih_fetch"]="Fetches research publication and grant data from the NIH"
            ["core_fetch"]="Fetches research publications from CORE API"
        )
        ;;

    # -------------------------------------------------------------------------
    process)
        local s3_prefix=scripts/process
        local env_vars='Variables={SECRET_NAME=prod/App/fetch,AWS_REGION_NAME=us-east-1,S3_BUCKET=REDACTED_S3_BUCKET,S3_PROCESSED_FOLDER=processed,S3_SOURCE_FOLDER=raw}'
        local timeout=300
        local memory_mb=512

        # Publish shared layers used by process functions
        local process_layer vader_layer
        process_layer=$(_publish_layer \
            "python311-process-layer" \
            "$s3_prefix" \
            "Dependencies for process functions") || return 1
        vader_layer=$(_publish_layer \
            "vader-sentiment" \
            "scripts/layers/vader" \
            "VADER sentiment analysis library") || return 1
        echo ""

        declare -A functions=(
            ["article_process"]="article.zip|$process_layer $vader_layer"
            ["gho_process"]="gho.zip|$process_layer"
            ["ihme_process"]="ihme.zip|$process_layer"
            ["trials_process"]="trials.zip|$process_layer"
            ["core_process"]="core_process.zip|$process_layer"
            ["gho_metadata"]="gho_lookup.zip|$process_layer"
            ["ihme_metadata"]="ihme_lookup.zip|$process_layer"
            ["icd_metadata"]="icd_lookup.zip|$process_layer"
            ["icd_catalogue"]="icd_catalogue.zip|$process_layer"
        )
        declare -A descriptions=(
            ["article_process"]="Parses and normalises raw news articles into structured records"
            ["gho_process"]="Transforms raw GHO indicator data into a structured format"
            ["ihme_process"]="Processes IHME (Global Burden of Disease) dataset into structured records"
            ["trials_process"]="Normalises and enriches raw clinical trial records"
            ["core_process"]="Processes and upserts CORE research publications into parquet"
            ["gho_metadata"]="Builds and refreshes the GHO indicator lookup/reference table"
            ["ihme_metadata"]="Builds and refreshes the IHME cause/metric lookup table"
            ["icd_metadata"]="Builds and refreshes the ICD diagnosis code lookup table"
            ["icd_catalogue"]="Builds and refreshes the ICD catalogue table"
        )
        ;;

    # -------------------------------------------------------------------------
    rds)
        local s3_prefix=scripts/rds
        local env_vars='Variables={SECRET_NAME=prod/App/fetch,AWS_REGION_NAME=us-east-1,S3_BUCKET=REDACTED_S3_BUCKET,S3_PROCESSED_FOLDER=processed}'
        local timeout=300
        local memory_mb=512

        # Publish all layers used by rds functions
        local process_layer pg8000_layer polars_layer vader_layer psycopg2_layer
        process_layer=$(_publish_layer \
            "python311-process-layer" \
            "scripts/process" \
            "Dependencies for process functions") || return 1
        pg8000_layer=$(_publish_layer \
            "pg8000-layer" \
            "scripts/layers/pg8000" \
            "pg8000 PostgreSQL driver") || return 1
        vader_layer=$(_publish_layer \
            "vader-sentiment" \
            "scripts/layers/vader" \
            "VADER sentiment analysis library") || return 1
        psycopg2_layer=$(_publish_layer \
            "psycopg2" \
            "scripts/layers/psycopg2" \
            "psycopg2 PostgreSQL adapter") || return 1
        echo ""

        declare -A functions=(
            ["rds_pipeline"]="rds_pipeline.zip|$pg8000_layer $process_layer $vader_layer"
            ["rds_schema_init"]="rds_schema_init.zip|$psycopg2_layer"
        )
        declare -A descriptions=(
            ["rds_pipeline"]="Runs the full RDS ingestion pipeline (Polars + pg8000 + VADER)"
            ["rds_schema_init"]="Initialises or migrates the RDS schema via psycopg2"
        )
        ;;

    # -------------------------------------------------------------------------
    orchestrator)
        local s3_prefix=scripts/orchestrator
        local env_vars='Variables={AWS_REGION_NAME=us-east-1}'
        local timeout=60
        local memory_mb=128

        declare -A functions=(
            ["daily_orchestrator"]="daily_orchestrator.zip|"   # no layers
        )
        declare -A descriptions=(
            ["daily_orchestrator"]="Triggers and coordinates the daily fetch/process pipeline"
        )
        ;;

    # -------------------------------------------------------------------------
    *)
        echo "❌ Unknown deploy type '$deploy_type'."
        echo "   Usage: bash cli_deployment fetch|process|rds|orchestrator"
        return 1
        ;;
    esac

    echo "🚀 Deploying: $deploy_type (s3://$_S3_bucket/$s3_prefix)"
    echo ""

    for func_name in "${!functions[@]}"; do
        local entry="${functions[$func_name]}"
        local s3_key="${entry%%|*}"        # everything before the first |
        local layer_arns="${entry##*|}"    # everything after the last  |

        _upsert_function \
            "$func_name" \
            "$_S3_bucket" \
            "$s3_prefix/$s3_key" \
            "$layer_arns" \
            "$env_vars" \
            "$timeout" \
            "$memory_mb" \
            "${descriptions[$func_name]}" || return 1
    done

    echo ""
    echo "========================================"
    echo "✅ All $deploy_type functions deployed successfully."
    echo "========================================"
}