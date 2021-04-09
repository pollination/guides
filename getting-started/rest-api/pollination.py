"""
A module demonstrating how to wrap the Pollination (api.pollination.cloud) REST
API for use in a custom application.
"""
import os
import httpx
from pprint import pprint as print
from dataclasses import dataclass, asdict
from queenbee.io.inputs.job import JobPathArgument
from typing import List


class Payload:
    class Base:
        def to_dict(self):
            return asdict(self)

    @dataclass
    class Create(Base):
        """
        Payload to create a Pollination project.

        Schema: https://api.pollination.cloud/redoc#operation/create_project
        """
        name: str
        description: str
        public: bool

    @dataclass
    class RecipeFilter(Base):
        """
        Payload for adding a recipe to a project.

        # operation/create_project_recipe_filter
        Schema: https://api.pollination.cloud/redoc
        """
        owner: str
        name: str
        tag: str

    @dataclass
    class Artifact(Base):
        """
        Payload for adding a file to a project.

        Schema: https://api.pollination.cloud/redoc#operation/create_artifact
        """
        key: str

        def upload(self, url: str, fields: dict) -> httpx.Response:
            """
            Upload the local file indicated by `self.key` to the bucket pointed
            to by `url` using the authorization in `fields`.

            Assumes a file named `self.key` exists in the directory where
            `python` is executed.

            We use a new httpx client here because the files are actually
            sent to Pollination's bulk storage server which is hosted on a
            different domain.
            """
            with open(self.key, 'rb') as fp:
                files = {'file': (self.key, fp)}

                res = httpx.post(
                    url=url, data=fields, files=files)

            return res

    @dataclass
    class Job(Base):
        """
        Minimal payload for creating a job.

        Schema: https://api.pollination.cloud/redoc#operation/create_job
        """
        source: str
        # A job can be started with many argument lists to run the same simulation
        # with different parameters.
        # Hence, the arguments must be a list of lists
        arguments: List[List[JobPathArgument]]

        def to_dict(self):
            d = asdict(self)
            args = []

            for group in self.arguments:
                g = []
                for arg in group:
                    g.append(arg.to_dict())
                args.append(g)

            d['arguments'] = args

            return d


class PollinationClient(httpx.Client):
    """
    An HTTP client specific to the Pollination REST API.
    """
    class Routes:
        """
        Simple container class for Pollination API routes.
        """
        var_name = '/{name}'
        var_owner = '/{owner}'
        var_job_id = '/{job_id}'
        var_run_id = '/{run_id}'
        var_output_name = '/{output_name}'

        artifacts = '/artifacts'
        downloads = '/downloads'
        filters = '/filters'
        jobs = '/jobs'
        outputs = '/outputs'
        recipes = '/recipes'
        runs = '/runs'
        user = '/user'

        accounts = '/accounts'
        accounts_name = accounts + var_name

        projects = '/projects'
        project_create = projects + var_owner
        project_add_recipe = (
            projects + var_owner + var_name + recipes + filters
        )
        project_add_artifact = projects + var_owner + var_name + artifacts
        project_jobs = projects + var_owner + var_name + jobs
        project_job_by_id = project_jobs + var_job_id
        project_job_artifacts = project_job_by_id + artifacts
        project_job_artifacts_download = project_job_artifacts + downloads
        project_runs = projects + var_owner + var_name + runs
        project_run_output = (
            project_runs + var_run_id + outputs + var_output_name
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.base_url = 'https://api.pollination.cloud'

        self.headers['x-pollination-token'] = os.environ['POLLINATION_API_KEY']

        self.organization = os.environ['POLLINATION_ORG']

    def _org_endpoint(self):
        return (
            self
            .Routes
            .accounts_name
            .format_map(dict(name=self.organization))
        )

    def get_organization(self) -> dict:
        res = self.get(self._org_endpoint())

        return res

    def create_project(self, body: Payload.Create) -> dict:
        endpoint = self.Routes.project_create.format_map(
            dict(owner=self.organization))

        res = self.post(endpoint, json=body.to_dict())

        return res

    def add_recipe_to_project(self, project_name: str, body: Payload.RecipeFilter):
        """
        Add a recipe to a project via a filter that will select the recipe
        from the set of recipes which are visible to the current account.
        """
        endpoint = (
            self
            .Routes
            .project_add_recipe
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name
                )
            )
        )

        res = self.post(endpoint, json=body.to_dict())

        return res

    def add_file_to_project(self, project_name: str, body: Payload.Artifact) -> dict:
        """
        Add a file to a project for use in simulations.

        Because the files are stored in a cloud storage provider, we must first
        ask the Pollination API to generate a storage link for us, then put
        the file's content to that location.
        """
        endpoint = (
            self
            .Routes
            .project_add_artifact
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name
                )
            )
        )

        res = self.post(endpoint, json=body.to_dict())

        res_body = res.json()

        res = body.upload(res_body['url'], res_body['fields'])

        return res if res.status_code == 204 else res.raise_for_status()

    def create_job(self, project_name: str, body: Payload.Job):
        endpoint = (
            self
            .Routes
            .project_jobs
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name
                )
            )
        )

        res = self.post(endpoint, json=body.to_dict())

        return res

    def get_job(self, project_name: str, job_id: str):
        endpoint = (
            self
            .Routes
            .project_job_by_id.format_map(
                dict(
                    owner=self.organization,
                    name=project_name,
                    job_id=job_id
                )
            )
        )

        res = self.get(endpoint)

        return res

    def list_jobs(self, project_name: str):
        endpoint = self.Routes.project_jobs.format_map(
            dict(owner=self.organization, name=project_name))

        res = self.get(endpoint)

        return res

    def list_job_artifacts(self, project_name: str, job_id: str):
        endpoint = (
            self
            .Routes
            .project_job_artifacts
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name,
                    job_id=job_id
                )
            )
        )

        res = self.get(endpoint)

        return res

    def get_job_artifact_link(self, project_name: str, job_id: str, artifact_path: str):
        """
        Schema: https://api.pollination.cloud/redoc#operation/download_job_artifact
        """
        endpoint = (
            self
            .Routes
            .project_job_artifacts_download
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name,
                    job_id=job_id
                )
            )
        )

        endpoint += f'?path={artifact_path}'

        res = self.get(endpoint)

        return res

    def get_runs(self, project_name: str, job_id: str):
        """
        Schema: https://api.pollination.cloud/redoc#operation/list_runs
        """
        endpoint = (
            self
            .Routes
            .project_runs
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name
                )
            )
        )

        endpoint += f'?job_id={job_id}'

        res = self.get(endpoint)

        return res

    def get_run_output(self, project_name: str, run_id: str, output_name: str):
        endpoint = (
            self
            .Routes
            .project_run_output
            .format_map(
                dict(
                    owner=self.organization,
                    name=project_name,
                    run_id=run_id,
                    output_name=output_name
                )
            )
        )

        res = self.get(endpoint)

        return res
