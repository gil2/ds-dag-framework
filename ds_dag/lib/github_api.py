import re
import time
from datetime import datetime
from enum import Enum
from logging import Logger
from typing import List

import jwt
import requests

from ds_dag.lib.api_utils import get_from_api

logger = Logger(__file__)

APP_OWNER = "wix-private"
BASE_BRANCH = 'master'


class GithubApiException(Exception):
    pass


class GithubApiPaths(Enum):
    github_api_url = "https://api.github.com"
    INSTALLATIONS_URL = f"{github_api_url}/app/installations"
    REPOS_URL = f"{github_api_url}/repos/{APP_OWNER}/{{repository}}"
    READ_DIRECTORY_TEMPLATE = f'{REPOS_URL}/contents/{{directory}}?ref={{ref}}'
    READ_FILE_TEMPLATE = f'{REPOS_URL}/contents/{{filename}}?ref={{ref}}'
    WORKFLOWS_URL = f'{REPOS_URL}/actions/workflows/{{workflow_id}}/dispatches'
    WORKFLOWS_LIST = f'{REPOS_URL}/actions/workflows'
    WORKFLOWS_RUNS_URL = f'{REPOS_URL}/actions/workflows/{{workflow_id}}/runs'
    WORKFLOWS_RUN_URL = f'{REPOS_URL}/actions/runs/{{workflow_run_id}}'

    @classmethod
    def installations_url(cls) -> str:
        return cls.INSTALLATIONS_URL.value

    @classmethod
    def read_file_url(cls, repository: str, filename: str, ref: str = 'main'):
        return cls.READ_FILE_TEMPLATE.value.format(repository=repository, filename=filename, ref=ref)

    @classmethod
    def read_directory_url(cls, repository: str, directory: str, ref: str = 'main'):
        return cls.READ_DIRECTORY_TEMPLATE.value.format(repository=repository, directory=directory, ref=ref)


class GithubApi:
    def __init__(self, private_key: str, logger: Logger, repository: str = None):
        self.client_id = "Iv23likOg5hnSrT0XKla"
        self.app_name = "wix-ml-docker-builder"
        self.app_owner = APP_OWNER

        self.logger = logger
        self.private_key = private_key
        self.installation_access_token = self.get_installation_access_token()

        self.json_headers = self._template_headers(access_token=self.installation_access_token,
                                                   content_type='application/vnd.github+json')
        self.json_headers.update({"X-GitHub-Api-Version": "2022-11-28"})
        self.raw_headers = self._template_headers(access_token=self.installation_access_token,
                                                  content_type='application/vnd.github.v3.raw')

        self.repository = repository

    @staticmethod
    def _template_headers(access_token: str, content_type: str):
        return {
            'Authorization': f'Bearer {access_token}',
            'Accept': content_type
        }

    def get_jwt_token(self):
        now_int = int(time.time())
        jwt_payload = {
            'iat': now_int,
            'exp': now_int + 600,
            'iss': self.client_id
        }
        return jwt.encode(jwt_payload, self.private_key, algorithm='RS256')

    def get_installation_access_token(self):
        headers = self._template_headers(access_token=self.get_jwt_token(),
                                         content_type='application/vnd.github+json')

        installations = get_from_api(GithubApiPaths.installations_url(), headers=headers)
        installations = [i for i in installations if i.get('account').get('login') == 'wix-private']
        installation_id = installations[0]['id']

        access_token_url = f'{GithubApiPaths.installations_url()}/{installation_id}/access_tokens'

        access_token = requests.post(access_token_url, headers=headers).json()
        return access_token['token']

    def read_file_from_github(self, repository: str, filename: str, ref: str = BASE_BRANCH) -> str:
        self.logger.info(f"Reading file {filename} from repository {repository}")

        return get_from_api(GithubApiPaths.read_file_url(repository=repository, filename=filename, ref=ref),
                            headers=self.raw_headers)

    def list_directory_contents(self, repository: str, directory: str, ref: str = BASE_BRANCH) -> list:
        self.logger.info(f"Listing contents of directory {directory} from repository {repository}")

        return get_from_api(GithubApiPaths.read_directory_url(repository=repository, directory=directory, ref=ref),
                            headers=self.raw_headers)

    def trigger_workflow(self, repository: str, workflow_id: str, ref: str = BASE_BRANCH, inputs: dict = None):
        self.logger.info(f"Triggering workflow {workflow_id} in repository {repository}")

        api_url = GithubApiPaths.WORKFLOWS_URL.value.format(repository=repository, workflow_id=workflow_id)
        self.logger.info(f"api_url: {api_url}")

        response = requests.post(url=api_url, headers=self.raw_headers, json={
            "ref": ref, "inputs": inputs
        })
        if response.status_code != 204:
            raise GithubApiException(f"Failed to trigger workflow: {response.content}")

        self.logger.info(f"Workflow triggered successfully")
        self.logger.info(f"response status code: {response.status_code}")
        self.logger.info(f"response text: {response.text}")

    def list_workflows(self, repository: str) -> List[dict]:
        self.logger.info(f"Listing workflows in repository {repository}")

        return get_from_api(GithubApiPaths.WORKFLOWS_LIST.value.format(repository=repository), headers=self.json_headers)

    def get_workflow_run(self, repository: str, workflow_run_id: str) -> dict:
        self.logger.info(f"Getting workflow run {workflow_run_id} in repository {repository}")

        return get_from_api(GithubApiPaths.WORKFLOWS_RUN_URL.value.format(repository=repository, workflow_run_id=workflow_run_id),
                            headers=self.raw_headers)

    def get_workflow_run_id_and_html_url(self, repository: str, workflow_id: str, triggered_time: datetime) -> tuple:
        max_retries = 10
        retry_count = 0
        run_id = None
        self.logger.info(f"Getting workflow run {workflow_id} in repository {repository}")
        while retry_count < max_retries:
            response_dict = get_from_api(GithubApiPaths.WORKFLOWS_RUNS_URL.value.format(repository=repository, workflow_id=workflow_id),
                                         headers=self.raw_headers)
            runs = response_dict['workflow_runs']

            # Find the latest run that was created after the workflow was triggered
            for run in runs:
                run_created_at = datetime.strptime(run['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                if run_created_at > triggered_time:
                    run_id = run['id']
                    run_html_url = run['html_url']
                    self.logger.info(f"Found new run_id: {run_id} with {run_html_url}")
                    return run_id, run_html_url

            self.logger.info("No new runs found yet, retrying...")
            retry_count += 1
            time.sleep(5)  # wait for 5 seconds before retrying

        if not run_id:
            self.logger.error("Failed to get the new workflow run ID")
            raise Exception("Failed to get the new workflow run ID")



# Function to trigger GitHub Actions workflow
def trigger_github_workflow(
        github_token,
        repository,
        workflow_id,
        inputs,
        ti
):
    github_api = GithubApi(github_token, logger)
    triggered_time = datetime.utcnow()
    github_api.trigger_workflow(
        repository=repository,
        workflow_id=workflow_id,
        inputs= inputs,
    )

    run_id, html_url = github_api.get_workflow_run_id_and_html_url(
        repository=repository,
        workflow_id=workflow_id,
        triggered_time=triggered_time,
    )

    ti.xcom_push(key='github_actions_logs', value=html_url)
    return run_id



# Function to check the status of the GitHub Actions workflow
def check_github_workflow_status(repository, run_id, github_token):
    logger.info(f"Workflow run ID: {run_id}")

    github_api = GithubApi(github_token, logger)
    workflow_run = github_api.get_workflow_run(
        repository=repository,
        workflow_run_id=run_id,
    )

    status = workflow_run['status']
    if status == 'completed':
        conclusion = workflow_run['conclusion']
        if conclusion == 'success':
            return True
        else:
            msg = f"Workflow failed with conclusion: {conclusion}. For more info see {workflow_run['html_url']}"
            raise Exception(msg)

    return False

def is_valid_commit_sha(ref):
    """
    Check if the provided ref is a valid commit SHA.
    A valid commit SHA is a 40-character long hexadecimal string.
    """
    return bool(re.match(r'^[0-9a-f]{40}$', ref))
