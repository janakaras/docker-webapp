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
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World from the search service!"

@app.route("/search")
def search():
    date = request.args.get("date")
    duration = request.args.get("duration")

    if date == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide the start of your stay."}', status=400, mimetype="application/json")

    if duration == None: 
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a duration for your stay."}', status=400, mimetype="application/json")
       
    duration = int(duration)

    # connect and setup database 
    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reservations (id text, name text, start text, duration int, vip int)")
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters int)")

    # Check if there is a colliding reservation

    cursor.execute("SELECT DISTINCT name, squaremeters FROM appartments")
    appartments = cursor.fetchall() 

    exclude_appartments = []

    for appartment in appartments:
        name = appartment[0]
        
        cursor.execute("SELECT start, duration FROM reservations WHERE name = ?", (name,))
        reservations = cursor.fetchall()
        for reservation in reservations:
            string_date = reservation[0]
            int_duration = int(reservation[1])
            date_date_existing = datetime.strptime(str(string_date), '%Y%m%d')
            date_date_new = datetime.strptime(str(date), '%Y%m%d')

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
                    exclude_appartments.append(name)
                    break
                
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

                if str(date) == str(string_date_existing):
                    exclude_appartments.append(name)
                    break
                
                date_date_existing = date_date_existing + timedelta(days=1)

    for appartment in appartments:
        if appartment[0] in exclude_appartments:
            appartments.remove(appartment)

    
    if len(appartments) == 0:
        return Response(
             '{"result": false, "error": 2, "description": "No results. Try again with another date."}', status=400, mimetype="application/json")

    results = "<h3>Apartment, Size (in squaremeters)</h3>"
    for appartment in appartments:
        results += f"<p> {appartment[0]}, {appartment[1]}</p>\n"

    connection.close()

    return results

@app.route("/reservations")
def reservations():
    if os.path.exists("/home/data/search.db"):

        # connect to db 
        connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
        cursor = connection.cursor()

        # create table if it does not exist yet
        cursor.execute("CREATE TABLE IF NOT EXISTS reservations (id text, name text, start text, duration int, vip int)")
        
        # get data 
        cursor.execute("SELECT * FROM reservations")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"reservations": rows})

    return json.dumps({"reservations": []})

@app.route("/appartments")
def appartments():
    if os.path.exists("/home/data/search.db"):

        # connect to db 
        connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
        cursor = connection.cursor()

        # create table if it does not exist yet
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, start text, duration int, vip int)")
        
        # get data 
        cursor.execute("SELECT * FROM appartments")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"appartments": rows})

    return json.dumps({"appartments": []})

def appartment_added(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]
    name = data["name"]
    size = data["size"]

    logging.info(f"Adding appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (id, name, size))
    cursor.close()
    connection.close()

def appartment_removed(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]
    name = data["name"]

    logging.info(f"Removing appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM appartments WHERE id = ?", (id, ))
    cursor.close()
    connection.close()

def reservation_added(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]
    name = data["name"]
    start = data["start"]
    duration = data["duration"]
    vip = data["vip"]

    logging.info(f"Adding reservation {id}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO reservations VALUES (?, ?, ?, ?, ?)", (id, name, start, duration, vip))
    cursor.close()
    connection.close()

def reservation_removed(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]

    logging.info(f"Removing reservation {id}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM reservations WHERE id = ?", (id, ))
    cursor.close()
    connection.close()

def connect_to_mq():
    while True:        
        time.sleep(10)

        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        except Exception as e:
            logging.warning(f"Could not start listening to the message queue, retrying...")

def listen_to_events(channel):
    channel.start_consuming()

def register(): 
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("search", address="search", port=5000)
            break
        except (ConnectionError, consul.ConsulException): 
            logging.warning('Consul is down, reconnecting...') 
            time.sleep(5) 

def deregister(): 
    connection = consul.Consul(host='consul', port=8500)
    connection.agent.service.deregister("search", address="search", port=5002)

def find_service(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True) 
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None



if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", level=1 * 10)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)

    logging.info("Start.")

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

    # reservations
    channel.exchange_declare(exchange="reservations", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="reservations", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=reservation_added, auto_ack=True)
    logging.info("Waiting for messages.")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="reservations", queue=queue_name, routing_key="removed")
    channel.basic_consume(queue=queue_name, on_message_callback=reservation_removed, auto_ack=True)
    logging.info("Waiting for messages.")    


    thread = threading.Thread(target=listen_to_events, args=(channel,), daemon=True)
    thread.start()

    # Verify if database has to be initialized
    database_is_initialized = False
    if os.path.exists("/home/data/search.db"):
        database_is_initialized = True
    else:
        connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
        cursor = connection.cursor()
        
        # appartments
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters int)")
        address, port = find_service("appartments")
        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/appartments")
            data = response.json()

            for entry in data["appartments"]:
                cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (entry["id"], entry["name"], entry["squaremeters"]))

            database_is_initialized = True     

        # reservations
        cursor.execute("CREATE TABLE IF NOT EXISTS reservations (id text, name text, start text, duration int, vip int)")
        address, port = find_service("reserve")
        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/reservations")
            data = response.json()

            for entry in data["reservations"]:
                cursor.execute("INSERT INTO reservations VALUES (?, ?, ?, ?, ?)", 
                (entry["id"], entry["name"], entry["start"], entry["duration"], entry["vip"]))

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