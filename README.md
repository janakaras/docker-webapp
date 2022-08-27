# Docker Webapp

## Overview 

* Project for university course "Contemporary Software Development" @unibz. 
* A Webapp composed of microservices. 
* Each microservices consists of a flask app residing in a Docker container. 
* Docker-Compose is used to start the app.  

## Microservices 

* Apartments: Manages a database of registered apartments.
* Search: Shows available apartments for a specified timeframe.
* Reserve: Manages reservations, toggles apartments between available and unavailable for specified timeframe. 
* Gateway: Forwards requests to the correct microservice. 

## Details 

Check out the project on localhost, port 5004. You will be on the homepage.
This is the **Gateway** Microservice, which forwards the following commands to the right microservices: 
    + /appartments
    + /appartments/appartments
    + /appartments/add
    + /appartments/remove
    + /search
    + /reserve
    + /reserve/reservations
    + /reserve/add
    + /reserve/remove





