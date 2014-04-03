#hydra-java-mongo

The main propose of this class is manage a mongodb connection using hydra as a source of the mongo server ip.
Every request check if the list of servers has changes, if this happens create a new connection to mongo
database with the new server address list. The previous connection remains open until the next hydra server change
in order to fulfills the running connection requests.  