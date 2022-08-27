import sqlite3
import logging
import pika
import time
import threading
import json
from flask import Flask
from flask import request
from flask import Response
import consul
import os
import requests
import time
import uuid
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World from reserve!"

@app.route("/add")
def add():
    id = uuid.uuid4()
    name = request.args.get("name")
    start = request.args.get("start")
    duration = request.args.get("duration")
    vip = request.args.get("vip")

    if name == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide the name of the appartment you want to reserve."}', status=400, mimetype="application/json")

    if start == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide the start of your stay."}', status=400, mimetype="application/json")

    if duration == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide the duration of your stay."}', status=400, mimetype="application/json")

    if vip == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not state if you want a vip stay or not."}', status=400, mimetype="application/json")
    
    if int(vip) < 0 or int(vip) > 1: 
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because vip has to be 0 (no vip stay) or 1 (vip stay)."}', status=400, mimetype="application/json")
    
    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reservations (id text, name text, start text, duration int, vip int)")
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters int)")


    # Check if reservation already exists
    cursor.execute("SELECT COUNT(id) FROM reservations  WHERE name = ? AND start = ? AND duration = ?", (name, start, duration))
    already_exists = cursor.fetchone()[0]
    if already_exists > 0:
        return Response('{"result": false, "error": 2, "description": "Cannot proceed because this reservation already exists"}', status=400, mimetype="application/json")

    # Check if apartment exists 
    cursor.execute("SELECT name FROM appartments")
    apartments = cursor.fetchall() 
    found = False 
    for apartment in apartments: 
        if name == apartment[0]:
            found = True
            break
    if found == False: 
        return Response('{"result": false, "error": 3, "description": "Cannot proceed because this apartment does not exists"}', status=400, mimetype="application/json")

    # Check if there is a colliding reservation
    cursor.execute("SELECT start, duration FROM reservations WHERE name = ?", (name,))
    reservations = cursor.fetchall()
    for reservation in reservations:
        string_date = reservation[0]
        int_duration = int(reservation[1])
        date_date_existing = datetime.strptime(str(string_date), '%Y%m%d')
        date_date_new = datetime.strptime(str(start), '%Y%m%d')

        for i in range(int_duration + 1):
            year_new = date_date_new.year
            month_new = date_date_new.month
            day_new = date_date_new.day

            string_date_new = ""

            if month_new < 10: 
                string_date_new += str(year_new) + '0' + str(month_new)
            else: 
                string_date_new += str(year_new) + str(month_new)

            if day_new < 10:
                string_date_new+= '0' + str(day_new)
            else: 
                string_date_new+= str(day_new)

            if str(string_date) == str(string_date_new):
                return Response('{"result": false, "error": 4, "description": "Cannot proceed because this appartment is already reserved in that timeperiod"}', status=400, mimetype="application/json")
            
            date_date_new = date_date_new + timedelta(days=1)

        for i in range(int_duration + 1):
            year_existing = date_date_existing.year
            month_existing = date_date_existing.month
            day_existing = date_date_existing.day

            string_date_existing = ""

            if month_existing < 10: 
                string_date_existing += str(year_existing) + '0' + str(month_existing)
            else: 
                string_date_existing += str(year_existing) + str(month_existing)

            if day_existing < 10:
                string_date_existing+= '0' + str(day_existing)
            else: 
                string_date_existing+= str(day_existing)

            if str(start) == str(string_date_existing):
                return Response('{"result": false, "error": 4, "description": "Cannot proceed because this appartment is already reserved in that timeperiod"}', status=400, mimetype="application/json")
            
            date_date_existing = date_date_existing + timedelta(days=1)

    # Add reservation
    cursor.execute("INSERT INTO reservations VALUES (?, ?, ?, ?, ?)", (str(id), name, start, duration, vip))
    cursor.close()
    connection.close()

    # Notify everybody that the reservation was added
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="reservations", exchange_type="direct")
    channel.basic_publish(exchange="reservations", routing_key="added", body=json.dumps(
        {"id": str(id), "name": name, "start": start, "duration": duration, "vip": vip}))
    connection.close()

    return Response('{"result": true, description="Reservation was added successfully."}', status=201, mimetype="application/json")

@app.route("/remove")
def remove():
    id = request.args.get("id")

    if id == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide the ID of the reservation you want to remove."}', status=400, mimetype="application/json")

    # connect and setup the database
    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reservations (id text, name text, start text, duration int, vip int)")


    # get id if it exists and remove reservation
    cursor.execute("SELECT id FROM reservations WHERE id = ?", (id,))
    existing = cursor.fetchone()
    if existing is None:
        return Response('{"result": false, "error": 2, "description": "Cannot proceed because this appartment does not exist"}', status=400, mimetype="application/json")
    else:
        id =  existing[0]
        cursor.execute("DELETE FROM reservations WHERE id = ?", (id,))

    cursor.close()
    connection.close()

    # Notify everybody that the reservation was removed
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="reservations", exchange_type="direct")
    channel.basic_publish(exchange="reservations", routing_key="removed", body=json.dumps({"id": str(id)}))
    connection.close()

    return Response('{"result": true, "description": "Reservation was removed successfully."}', status=400, mimetype="application/json")


@app.route("/reservations")
def reservations():
    if os.path.exists("/home/data/reservations.db"):

        # connect to db 
        connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
        cursor = connection.cursor()

        # create table if it does not exist yet
        cursor.execute("CREATE TABLE IF NOT EXISTS reservations (id text, name text, start text, duration int, vip int)")
        
        # get data 
        cursor.execute("SELECT * FROM reservations")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"reservations": rows})

    return json.dumps({"reservations": []})

def appartment_added(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]
    name = data["name"]
    size = data["size"]

    logging.info(f"Adding appartment {name}...")

    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (id, name, size))
    cursor.close()
    connection.close()

def appartment_removed(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]
    name = data["name"]

    logging.info(f"Removing appartment {name}...")

    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM appartments WHERE id = ?", (id, ))
    cursor.close()
    connection.close()



def register(): 
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("reserve", address="127.0.0.1", port=5003)
            break
        except (ConnectionError, consul.ConsulException): 
            logging.warning('Consul is down, reconnecting...') 
            time.sleep(5) 

def connect_to_mq():
    while True:        
        time.sleep(10)

        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        except Exception as e:
            logging.warning(f"Could not start listening to the message queue, retrying...")

def listen_to_events(channel):
    channel.start_consuming()

def find_service(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True) 
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None

def deregister(): 
    connection = consul.Consul(host='consul', port=8500)
    connection.agent.service.deregister("reserve", address="reserve", port=5003)

if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    connection = connect_to_mq()

    channel = connection.channel()

    # appartments
    channel.exchange_declare(exchange="appartments", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_added, auto_ack=True)
    logging.info("Waiting for messages.")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="removed")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_removed, auto_ack=True)
    logging.info("Waiting for messages.")

    thread = threading.Thread(target=listen_to_events, args=(channel,), daemon=True)
    thread.start()

    # Verify if database has to be initialized
    database_is_initialized = False
    if os.path.exists("/home/data/reservations.db"):
        database_is_initialized = True
    else:
        connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
        cursor = connection.cursor()
        
        # appartments
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters int)")
        address, port = find_service("appartments")

        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/appartments")
            data = response.json()

            logging.info("Data received: " + data)

            for entry in data["appartments"]:
                cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (entry["id"], entry["name"], entry["squaremeters"]))

            database_is_initialized = True     
      
    if not database_is_initialized:
        logging.error("Cannot initialize database.")
    else:
        logging.info("Starting the web server.")

        try:
            app.run(host="0.0.0.0", threaded=True)
        finally:
            connection.close()
            deregister()