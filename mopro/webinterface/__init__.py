from flask import Flask, render_template, jsonify
import yaml
import os
import peewee
from collections import defaultdict

from ..config import config
from ..database import (
    initialize_database,
    CorsikaRun,
    CeresRun,
    CeresSettings,
    CorsikaSettings,
    Status,
    database
)

sortkey = defaultdict(
    int,
    walltime_exceeded=-1,
    failed=-2,
    created=0,
    queued=1,
    running=2,
    success=3,
)


app = Flask(__name__)
web_config = config.web_app.secret_key
app.secret_key = config['app'].pop('secret_key')
app.config.update(config.web)

initialize_database()


@app.before_request
def _db_connect():
    database.connect()


@app.teardown_request
def _db_close(exc):
    if not database.is_closed():
        database.close()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/states')
def states():
    states = [p.description for p in ProcessingState.select()]
    states = sorted(states, key=lambda k: sortkey[k])
    return jsonify({'status': 'success', 'states': states})


@app.route('/jobstats')
def jobstats():
    jobstats = list(
        ProcessingState.select(
            ProcessingState.description,
            Jar.version,
            XML.name.alias('xml'),
            peewee.fn.COUNT(Job.id).alias('n_jobs')
        )
        .join(Job)
        .join(XML)
        .switch(Job)
        .join(Jar)
        .group_by(Jar.version, XML.name, ProcessingState.description)
        .dicts()
    )
    return jsonify({'status': 'success', 'jobstats': jobstats})
<Paste>
