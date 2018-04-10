import os

from dotenv import load_dotenv
from fabric.api import *
from slackweb import Slack


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

slack = Slack(os.environ.get('SLACK_WEBHOOK_URL'))

env.use_ssh_config = True
env.hosts = ['deploy-fdm-server']
env.directory = '/home/fdm/sync'
env.venv_directory = 'venv'
env.activate = 'source ' + os.path.join(env.directory, env.venv_directory, 'bin/activate')
env.repo = 'https://www.github.com/KeltonKarboviak/NGAFID_Sync.git'


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
