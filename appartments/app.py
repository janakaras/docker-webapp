import sqlite3
import uuid
from flask import request
from flask import Flask
from flask import Response
import logging
import pika
import json
import consul
import time
import os
import time

app = Flask(__name__)


@app.route("/add")
def add():
    id = uuid.uuid4()
    name = request.args.get("name")
    size = request.args.get("size")

    if name == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a name for the appartment."}', status=400, mimetype="application/json")

    if size == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a size for the appartment."}', status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters int)")

    # Check if appartement already exists
    cursor.execute("SELECT COUNT(id) FROM appartments WHERE name = ? AND squaremeters = ?", (name, size))
    already_exists = cursor.fetchone()[0]
    if already_exists > 0:
        return Response('{"result": false, "error": 2, "description": "Cannot proceed because this appartment already exists"}', status=400, mimetype="application/json")

    # Add appartement
    cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (str(id), name, size))
    cursor.close()
    connection.close()

    # Notify everybody that the appartment was added
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")
    channel.basic_publish(exchange="appartments", routing_key="added", body=json.dumps({"id": str(id), "name": name, "size": size}))
    connection.close()

    return Response('{"result": true, description="Appartment was added successfully."}', status=201, mimetype="application/json")

@app.route("/remove")
def remove():
    name = request.args.get("name")

    if name == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide the name of the appartment you want to remove."}', status=400, mimetype="application/json")

    # connect to database
    connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters int)")

    # get id if the apartment exists
    cursor.execute("SELECT id FROM appartments WHERE name = ?", (name,))
    existing = cursor.fetchone()
    if existing is None:
        return Response('{"result": false, "error": 2, "description": "Cannot proceed because this appartment does not exist"}', status=400, mimetype="application/json")
    else:
        id =  existing[0]
        cursor.execute("DELETE FROM appartments WHERE id = ?", (id,))
    
    # remove appartment
    cursor.close()
    connection.close()

    # Notify everybody that the appartment was removed
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")
    channel.basic_publish(exchange="appartments", routing_key="removed", body=json.dumps({"id": str(id), "name": name}))
    connection.close()

    return Response('{"result": true, "description": "Appartment was removed successfully."}', status=400, mimetype="application/json")

    
@app.route("/")
def hello():
    return "Hello World from appartements!"

@app.route("/appartments")
def appartments():
    if os.path.exists("/home/data/appartments.db"):

        # connect to db 
        connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
        cursor = connection.cursor()

        # create table if it does not exist yet
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, start text, duration int, vip int)")
        
        # get data 
        cursor.execute("SELECT * FROM appartments")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"appartments": rows})

    return json.dumps({"appartments": []})

def register(): 
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("appartments", address="127.0.0.1", port=5001)
            break
        except (ConnectionError, consul.ConsulException): 
            logging.warning('Consul is down, reconnecting...') 
            time.sleep(5) 

if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    app.run(host="0.0.0.0", threaded=True)