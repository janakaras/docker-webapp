from flask import request
from flask import Flask, redirect
from flask import Response
import logging
import consul
import time
import requests

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World from the gateway!"

def find_service(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True) 
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None

# Appartments 
@app.route("/appartments")
def appartments():
    address, port = find_service("appartments")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/{request.query_string.decode('utf-8')}", code=302)

@app.route("/appartments/appartments")
def appartments_appartments():
    address, port = find_service("appartments")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/appartments?{request.query_string.decode('utf-8')}", code=302)


@app.route("/appartments/add")
def appartments_add():
    address, port = find_service("appartments")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/add?{request.query_string.decode('utf-8')}", code=302)
   
@app.route("/appartments/remove")
def appartments_remove():
    address, port = find_service("appartments")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/remove?{request.query_string.decode('utf-8')}", code=302)

# Search
@app.route("/search")
def search(): 
    address, port = find_service("search")
    url = request.url.replace(request.host_url, f"http://{address}:{port}/")
    logging.info(f"Requesting content from {url} ...")
    response = requests.get(url)
    return Response(response.content, response.status_code, mimetype="application/json")

# Appartments 
@app.route("/reserve")
def reserve():
    address, port = find_service("reserve")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/{request.query_string.decode('utf-8')}", code=302)

@app.route("/reserve/reservations")
def reserve_reservations():
    address, port = find_service("reserve")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/reservations?{request.query_string.decode('utf-8')}", code=302)


@app.route("/reserve/add")
def reserve_add():
    address, port = find_service("reserve")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/add?{request.query_string.decode('utf-8')}", code=302)
   
@app.route("/reserve/remove")
def reserve_remove():
    address, port = find_service("reserve")
    if address is None or not port:
         return Response(
        '{"result": false, "error": 1, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")
    else: 
        return redirect(f"http://{address}:{port}/remove?{request.query_string.decode('utf-8')}", code=302)

# consul
def register(): 
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("gateway", address="127.0.0.1", port=5004)
            break
        except (ConnectionError, consul.ConsulException): 
            logging.warning('Consul is down, reconnecting...') 
            time.sleep(5) 

if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    app.run(host="0.0.0.0", threaded=True)

