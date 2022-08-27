# CSE Microservices Jana Karas
## Achievements: 

Everything is done! 

Details: 

* **Appartments** Microservice: Added the parameter "size" so that the command is */add?name=...&size=...)"*
* **Appartments** Microservice: Added the functionality to remove an appartment so that the command is */remove?name=...)*
* Added the **Reserve** Microservice
* **Reserve** Microservice: Added the functionality to add a reservation so that the command is */add?name=...&start=yyyymmdd&duration=...&vip=1* 
    + Adding a reservation for a non-existing apartment is blocked
    + Adding a reservation that conflicts with another reservation is blocked
* **Reserve** Microservice: Added the functionality to remove a reservation so that the command is */remove?id=...* 
* **Search** Microservice: Added the functionality to search for appartments so that the command is */search?date=...&duration=...* 
    + Appartments that are already booked are not shown in the search results
* Added a **Gateway** Microservice
* The **Gateway** Microservice forwards the following commands to the right microservices: 
    + /appartments
    + /appartments/appartments
    + /appartments/add
    + /appartments/remove
    + /search
    + /reserve
    + /reserve/reservations
    + /reserve/add
    + /reserve/remove

## Ports

| Microservice | Port |
| ------------ | ------ |
| Gateway | 5004 |
| Appartments | 5001 |
| Search | 5002 |
| Reserve | 5003 | 




