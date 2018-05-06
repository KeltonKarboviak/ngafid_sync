# -*- coding: utf-8 -*-

from fabric.api import cd, env, run, task


env.use_ssh_config = True
env.hosts = ['deploy-fdm-server']
env.directory = '/home/fdm/sync'
env.repo = 'https://github.com/KeltonKarboviak/ngafid_sync.git'


@task(default=True)
def deploy():
    pull_changes()
    run_pipenv()
    update_permissions()


def pull_changes():
    with cd(env.directory):
        run('git pull')


def run_pipenv():
    with cd(env.directory):
        run('pipenv clean')
        run('pipenv install --deploy')


def update_permissions():
    with cd(env.directory):
        run('chmod 770 .env dbx_org_tokens.json')
