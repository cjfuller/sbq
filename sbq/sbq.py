import json
import os
import time

import requests

ACCOUNT = ""
PROJECT = ""
JOBS_URL = 'https://www.googleapis.com/bigquery/v2/projects/{project}/jobs'
CREDENTIALS_FILE = os.path.expanduser('~/.config/gcloud/credentials')
DRY_RUN = False

_params = {}


def params(add={}):
    _params.update(add)
    return _params


def account(set_to=None):
    global ACCOUNT
    if set_to:
        ACCOUNT = set_to
    return ACCOUNT


def project(set_to=None):
    global PROJECT
    if set_to:
        PROJECT = set_to
    return PROJECT


def dry_run(set_to=None):
    global DRY_RUN
    if set_to is not None:
        DRY_RUN = set_to
    return DRY_RUN


class QueryFailed(Exception):
    pass


def token():
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)['data'][0]['credential']['access_token']


def headers():
    return {
        'Authorization': 'Bearer {token}'.format(token=token()),
        'content-type': 'application/json',
    }


def refresh_token():
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)['data'][0]['credential']['refresh_token']


def try_to_refresh():
    os.system('gcloud auth activate-refresh-token {account} {token}'.format(
        account=account(), token=refresh_token()))


def format_api_request(query, dataset, table):
    return json.dumps({
        'configuration': {
            'query': {
                'allowLargeResults': True,
                'destinationTable': {
                    'datasetId': dataset,
                    'projectId': project(),
                    'tableId': table,
                },
                'query': query,
                'writeDisposition': 'WRITE_TRUNCATE'}}})


def check_status(job_id):
    url = JOBS_URL.format(project=project()) + '/' + job_id
    resp = requests.get(url, headers=headers()).json()

    return resp['status']['state'], resp['status'].get('errorResult', None)


def wait_for_completion(job_id, wait_time=1):
    status, errs = check_status(job_id)
    print("Waiting for job {id}.  Status is: {status}.".format(
        id=job_id, status=status))
    if status == 'DONE':
        if errs:
            raise QueryFailed(errs)
    else:
        print("Trying again in {t} seconds...".format(
            t=wait_time))
        time.sleep(wait_time)
        wait_for_completion(job_id, min(2*wait_time, 15))


def run_query(query, dataset, table, retry_on_auth_fail=True):
    query_repr = "{q}\n\n--> [{d}.{t}]\n\n".format(
        q=query, d=dataset, t=table)
    if dry_run():
        print("")
        print("Query dry run:")
        print(query_repr)
    else:
        print("")
        print("Running:")
        print(query_repr)
        resp = requests.post(
            JOBS_URL.format(project=project()),
            headers=headers(),
            data=format_api_request(query, dataset, table))
        if resp.status_code == 401 and retry_on_auth_fail:
            try_to_refresh()
            run_query(query, dataset, table, False)

        if resp.status_code > 299:
            raise QueryFailed(resp.__dict__)
        else:
            job_id = resp.json()['jobReference']['jobId']
            wait_for_completion(job_id)


def run_in_order(*queries):
    return (q() for q in queries)


def query(destination_table, destination_dataset=None):
    destination_dataset = destination_dataset or params().get(
        'output_dataset', None)

    def decorator(fn):
        def new_fn():
            query = fn().format(**params())
            return run_query(
                query,
                destination_dataset.format(**params()),
                destination_table.format(**params()))
        return new_fn
    return decorator
