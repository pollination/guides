"""
A script demonstrating the sequence of REST API calls to be made to use
Pollination (app.pollination.cloud) to create a simulation job that is
parameterized over multiple 3D models.
"""
import time
from queenbee.io.artifact_source import ProjectFolder
from queenbee.io.inputs.job import JobPathArgument
from urllib.request import urlretrieve
from pollination import PollinationClient, Payload

if __name__ == '__main__':
    # Create client
    client = PollinationClient()

    # Create project object and post
    project = Payload.Create(
        name='good-project',
        description='A very good project',
        public=False
    )
    res = client.create_project(project)
    print(res.json())

    # Create recipe filter and post
    recipe_filter = Payload.RecipeFilter(
        owner='ladybug-tools',
        name='daylight-factor',
        tag='0.5.6'
    )
    res = client.add_recipe_to_project(project.name, recipe_filter)
    print(res.json())

    # Create artifacts for 2 versions of a 3D model
    file_names = ['model1.hbjson', 'model2.hbjson']

    arguments = []

    for name in file_names:
        artifact = Payload.Artifact(key=name)
        res = client.add_file_to_project(project.name, artifact)
        print(res)

        # Construct an argument to the job for the 3D model that was uploaded
        project_artifact = ProjectFolder(path=artifact.key)
        model_argument = JobPathArgument(
            name='model',  # This corresponds to a named input from the recipe
            source=project_artifact
        )

        # Wrap each argument in its own list to parameterize
        arguments.append([model_argument])

    # Construct source URL for recipe
    recipe_source_url = '/'.join(
        [
            str(client.base_url),
            'registries',
            recipe_filter.owner,
            'recipe',
            recipe_filter.name,
            recipe_filter.tag
        ]
    )

    # Create a job
    job = Payload.Job(source=recipe_source_url, arguments=arguments)
    res = client.create_job(project.name, job)
    print(res.json())

    job_id = res.json()['id']

    # Poll the created job until it is in an end state
    remaining_polls = 5
    wait_sec = 60
    print(
        f'Waiting up to {remaining_polls * wait_sec} sec for job to finish...'
    )

    while remaining_polls > 0:
        res = client.get_job(project.name, job_id)
        body = res.json()
        job_status = body['status']['status']

        if job_status == 'Completed':
            print('Job finished!')
            break

        if job_status == 'Cancelled':
            print('Job cancelled!')
            exit()

        time.sleep(wait_sec)  # Wait for job to finish
        remaining_polls -= 1
        print(f'{remaining_polls} attempts remaining')

    # Select the runs that were created for the submitted Job
    res = client.get_runs(project.name, job_id)

    body = res.json()

    for run in body['resources']:
        run_id = run['id']
        # The selected recipe determines the outputs
        run_output_name = run['status']['outputs'][0]['name']

        # Get a download link for the run output
        res = client.get_run_output(project.name, run_id, run_output_name)
        url = res.json()

        # Save to a local file
        urlretrieve(url, f'{run_id}-{run_output_name}.zip')
