import os

from fabric.api import *


env.use_ssh_config = True
env.hosts = ['deploy-fdm-server']
env.directory = '/home/fdm/sync'
env.venv_directory = 'venv'
env.activate = 'source ' + os.path.join(env.directory, env.venv_directory, 'bin/activate')
env.repo = 'https://github.com/KeltonKarboviak/ngafid_sync.git'


@task(default=True)
def deploy():
    pull_changes()
    run_pip()
    update_permissions()


def pull_changes():
    with cd(env.directory):
        run('git pull')


def run_pip():
    with cd(env.directory), prefix(env.activate):
        run('pip install -r requirements.txt')


def update_permissions():
    with cd(env.directory):
        run('chmod 770 .env dbx_org_tokens.json')
