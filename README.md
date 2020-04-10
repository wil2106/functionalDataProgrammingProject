# functionalDataProgrammingProject
## Files :
* Broker : basic kafka consumer to test things (not very important)
* Consumer : kafka consumer that inserts data into a database and sends mail alerts
* DroneStatusProducer : kafka producer that will send drone status message each 10 seconds, bash file here to start mulitple of them
* InfractionProducer : kafka producer that waits for a message (the range to read) from the master to read csv file (not finished) and send it
* InfractionProducerClusterMaster : Program here to compute ranges for the csv file and send them to the InfractionProducers (synchronization with zookeeper)
* SingleInfractionProducer : kafka producer that reads the csv file and sends it line by line (this one should have been merged with InfractionProducer)
* stat.txt : dataviz notebook for stats
