import datetime
import json
import pymysql.cursors
import requests
from flask import render_template, redirect, request
from app import app
CONNECTED_NODE_ADDRESS = "http://127.0.0.1:8000"
posts = []
def fetch_posts():
    get_chain_address = "{}/chain".format(CONNECTED_NODE_ADDRESS)
    response = requests.get(get_chain_address)
    if response.status_code == 200:
        content = []
        chain = json.loads(response.content)
        for block in chain["chain"]:
            for tx in block["trans"]:
                tx["index"] = block["index"]
                tx["hash"] = block["prev_hash"]
                content.append(tx)
 global posts
        posts = sorted(content, key=lambda k: k['timestamp'],
                       reverse=True)
@app.route('/')
def index():
    fetch_posts()
return render_template('login.html',
                           title='WELCOME !',
                           posts=posts,
                           node_address=CONNECTED_NODE_ADDRESS,
                           readable_time=timestamp_to_string)
def timestamp_to_string(epoch_time):
    return datetime.datetime.fromtimestamp(epoch_time).strftime('%H:%M')
