How to deploy

1. Upload the zip fetch_function in your bucket under in the folder /scripts/fetch
2. Upload the zip process_function in your bucket under in the folder /scripts/process
3. Update if necessary the shell script cli_process_deploy.bash if any of these don't match your setup
4. Copy the shell script into AWS Cloudshell
5. Run it with `deploy fetch` or `deploy process`
6. Upload the files in data_raw_aws to the folder /raw
7. Set up EventBridge Rules: upload `EventBridgeRules.sh` to CloudShell, then run:

```bash
sed -i 's/\r//' EventBridgeRules.sh
bash EventBridgeRules.sh
```
This creates the following schedules (08:00 Zürich time = 06:00 UTC):

| Function | Schedule |
|---|---|
| `news_fetch` | Daily (Monday–Sunday) |
| `ct_us_fetch` | Daily (Monday–Sunday) |
| `current_fetch` | Weekly (Monday) |
| `nih_fetch` | Weekly (Monday) |
| `core_fetch` | Weekly (Monday) |

And the following S3 event pattern route rules:

| Rule | S3 Key Pattern | Target Lambda |
|---|---|---|
| `route-who-files` | `raw/*/who_*.json` | `article_process` |
| `route-news-files` | `raw/*/news_*.json` | `article_process` |
| `route-currentapi-files` | `raw/*/currentapi_*.json` | `article_process` |
| `route-clinical_trials_us-files` | `raw/*/clinical_trials_us_*.json` | `trials_process` |
| `route-core-files` | `raw/*/core_*.json` | `core_process` |
| `route-ctis-files` | `raw/*/CTIS_*.csv` | `trials_process` |
| `route-gho-files` | `raw/*/gho_*.json` | `gho_process` |
| `route-ihme-files` | `raw/*/IHME*.csv` | `ihme_process` |

**Note:** For S3 route rules to fire, EventBridge notifications must be enabled on the bucket.
The script handles this automatically, but if needed run:

> ```bash
> aws s3api put-bucket-notification-configuration \
>     --bucket REDACTED_S3_BUCKET \
>     --notification-configuration '{"EventBridgeConfiguration": {}}'
> ```

8. Test/Run the scripts.
