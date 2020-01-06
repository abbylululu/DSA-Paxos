# Distributed-Flight-Messages.Reservation-System

## I. Project Description
Implementing a distributed flight reservation service using the Paxos Algorithm to
maintain a replicated log of “reserve” and “cancel” events.\
The system consists of N sites. Each site plays the role of a Roles.Proposer, issuing proposals for “reserve”
and “cancel” operations. It also plays the role of an Roles.Acceptor, helping to determine which proposal
should be accepted for each log position. \
And, it plays the role of a Roles.Learner; it stores a replica of the log
in stable storage and updates this log with committed log entries. The Roles.Learner can also store a data
structure that contains the current reservations, but this data structure should not be stored in stable
storage 

## II. File Structure
A Paxos distributed flight reservation system by Java and it includes 7 files:
##### App.Host.java: 
> Implement the JSON input and user interface
> Learn lost log values
>Resolve Reserve/ Cancel conflicts
##### Roles.Proposer.java:
> Processing Roles.Proposer messages
> Implement the Synod Algorithm
##### Roles.Acceptor.java:
>Implement UDP receiving for 3 roles and connecting other 2 roles by blocking queue
>Processing Roles.Acceptor messages
##### Roles.Learner.java
>Continuously learn the incoming messages
##### Utils.SendUtils.java
##### Messages.Record.java
##### Messages.Reservation.java


## III. Project Design
### Algorithm:
Paxos Algorithm ans Synod Algorithm

### Data structures:
##### Roles.Proposer
>Playing the role of proposer in the main thread
##### Roles.Acceptor
>Playing the role of Roles.Acceptor and receive all messages for 3 roles in a child thread
##### Roles.Learner
>Playing the role of Roles.Learner and recording messages in a child thread
##### Utils.SendUtils
>Multithreaded UDP sender to ensure send the proposals concurrently
##### Messages.Record
> Utility class for recording the accepted log entry information
##### Messages.Reservation
> Utility class for storing reservation/canceling information


### Stable storage: 
1. Log: Write to local txt files after everytime update
2. Messages.Reservation data structures: Write to local txt files every 5 logs(Checkpoints)

### Sockets and Threads: 
Enable the multithreaded system to ensure the listening, sending and updating concurrently, 
the communication between threads implemented by BlockingQueue:
1. Main Thread: Roles.Proposer and UI 
2. Roles.Acceptor: Keeping listening the incoming messages from other sites
3. Utils.SendUtils: Using UDP to send message concurrently
4. Roles.Learner: Continuously recording the incoming message and updating log

## IV. Implementation
To be completed in the next few days

## V. Build
```shell
#!/bin/bash
rm -rf bin
mkdir bin

# complie java
javac *.java -cp ./json-simple-1.1.1.jar -Xlint:unchecked -d ./bin

# cp shell and library
cp run.sh bin/
cp json-simple-1.1.1.jar bin/

echo Done!
exit 0
```

## VI. Run
```shell
#!/bin/bash
# Run my program.

java -classpath .:json-simple-1.1.1.jar App.Host $1

exit 0
```


## VII. To Be Improved
#### Message Transportation System
> Currently, the message sent in String and due to the String processing flaws(split all string by space) and flight numbers,
the length of string may be different. The string should be changed to a independent data structure, which will 
simplify the manipulation of string and elegance.
>
> Meanwhile, the application of this data structure will simplify the recording of proposer's ip recording,
instead of sending multiple unnecessary messages.

#### Project Design
> There are lots of unnecessary and duplicates operations among different class files that should be merged or 
use static.

## Acknowledgments
* Professor P.
* Coffee
* People that I love :)
