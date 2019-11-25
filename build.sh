#!/bin/bash
rm -rf bin
mkdir bin

# complie java
javac ./src/*.java -cp ./json-simple-1.1.1.jar -Xlint:unchecked -d ./bin

# cp shell and library
cp run.sh bin/
cp json-simple-1.1.1.jar bin/

echo Done!
