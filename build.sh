#!/bin/bash
rm -rf bin
mkdir bin

# complie java
javac *.java -cp "./json-simple-1.1.1.jar:./javafx-base-14-ea+2.jar" -Xlint:unchecked -d ./bin

# cp shell and library
cp run.sh bin/
cp json-simple-1.1.1.jar bin/
cp javafx-base-14-ea+2.jar bin/

echo Done!
exit 0