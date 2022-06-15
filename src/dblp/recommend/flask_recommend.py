# coding=utf-8

import json
from flask import Flask, Response, jsonify, request
from recommend.main import main_single, main_all

app = Flask(__name__)


@app.route("/api/get-single-recommend/")
def get_recommendation():
    uid = request.args.get('uid')
    main_single(uid)
    return uid


@app.route("/api/get-all-recommend/")
def get_all_recommendation():
    main_all()


@app.route("/")
def test():
    return "这是一个测试接口"


app.run(port=5000, host='0.0.0.0')
