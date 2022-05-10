import os

from flask import Flask

from src.common.app_config import APP_CONFIG

app = Flask(__name__)


@app.route('/')
def info():
    version = os.environ['API_VERSION']
    return f"rest app up and running, v-{version}, config= {APP_CONFIG.sections()}"
