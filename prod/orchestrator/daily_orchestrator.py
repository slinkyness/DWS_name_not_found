import boto3, json
from datetime import datetime, timezone


def lambda_handler(event, context):
    client = boto3.client('lambda', region_name='us-east-1')
    s3 = boto3.client('s3', region_name='us-east-1')
    BUCKET = 'REDACTED_S3_BUCKET'

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    prefix = f'raw/{today}/'

    results = []
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get('Contents', [])

    for obj in objects:
        key = obj['Key']
        if 'news_' in key or 'currents_' in key:
            func = 'article_process'
        elif 'clinical_trials_us_' in key:
            func = 'trials_process'
        elif 'core_' in key:
            func = 'core_process'
        elif 'gho_' in key:
            func = 'gho_process'
        else:
            continue

        event_payload = {'Records': [{'s3': {'bucket': {'name': BUCKET}, 'object': {'key': key}}}]}
        r = client.invoke(FunctionName=func, Payload=json.dumps(event_payload).encode())
        body = json.loads(r['Payload'].read())
        results.append({'key': key, 'function': func, 'result': body})

    return {'processed': len(results), 'results': results}
