# jpmml-iris-kafka #

Frank Polet
October 24 2020

## Introduction ##

The purpose of this example is to illustrate how to operate a ML model (in PMML format) in a real time pipeline using kafka streams.

This PoC is based on the famous iris classification ml model.

The way how the PMML has been built is out of scope. We use the iris one as per example to illustrate the operating part.

## Description ##
We use two input kafka topics:

- "model"
- "iris"

The "model" topic stores the PMML XML information of the ML model. It can be loaded using the *ModelPublisher.java* class

The "iris" topic contains the real time information on iris features to be checked. Record format is csv and looks like this:

*`sepal_length,sepal_width,petal_length,petal_width,class`*

We use two output topics:

- "iris-virginica"
- "iris-others"

The ML model classifies the input data.

The kafka streams topology is composed of two elements: 

- the model stream. It tracks in real time any model change and make sure new model is used as soon as received

- the iris input stream. It tracks in real time any new iris record, classifies it as soon as received and, based on classification result, put the record in "iris-virginica" topic or in "iris-others".

**Notes**: 

The PMML management part has been clearly inspired by https://github.com/hkropp/jpmml-iris-example 

The class *JpmmlModelIris.java* is a refactoring of this example and it's only purpose is to compare results with the ones of the the *JpmmlModelKafka.java*


## Tests  ##

    // create the needed topics
    kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic iris
    kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic iris-virginica
    kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic iris-others
    
    // check module producer has publihed the model
    kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic model
    <?xml version="1.0"?>
    <PMML version="4.2" xmlns="http://www.dmg.org/PMML-4_2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.dmg.org/PMML-4_2 http://www.dmg.org/v4-2/pmml-4-2.xsd">
    [...]
     </MiningModel>
    </PMML>
    // testing by pushing the model via the class "ModelPublisher" and some data
    
    C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic iris
    >5.4,3.9,1.7,0.4,Iris-setosa
    >5.7,2.8,4.1,1.3,Iris-versicolor
    >6.3,3.3,6.0,2.5,Iris-virginica
    >Terminer le programme de commandes (O/N)Â ?
    ^C
    // check that records have been classified correctly
    
    C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic iris
    5.4,3.9,1.7,0.4,Iris-setosa
    6.3,3.3,6.0,2.5,Iris-virginica
    5.7,2.8,4.1,1.3,Iris-versicolor
    Processed a total of 3 messages
    Terminer le programme de commandes (O/N)Â ? o
    
    C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic iris-others
    5.4,3.9,1.7,0.4,Iris-setosa
    5.7,2.8,4.1,1.3,Iris-versicolor
    Processed a total of 2 messages
    Terminer le programme de commandes (O/N)Â ? o
    
    C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic iris-virginica
    6.3,3.3,6.0,2.5,Iris-virginica
    Processed a total of 1 messages
    Terminer le programme de commandes (O/N)Â ? o
    
    // execute again ModelPublisher ( simulate new model deployment )
    
    // check if new record still classified after having loaded new model
    C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic iris
    >6.1,3.0,4.6,1.4,Iris-versicolor
    // java program output
    
    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
    ****************************** <<-- at start loads model
    *** Initializing model ... ***
    *** Active Fields of Model ***
    *** Field Name: Sepal.Length
    *** Field Name: Sepal.Width
    *** Field Name: Petal.Length
    *** Field Name: Petal.Width
    ******************************
    % 'setosa': 1.0
    % 'versicolor': 0.0
    % 'virginica' : 0.0            <<-- after first input
    % 'setosa': 0.0
    % 'versicolor': 1.0
    % 'virginica' : 0.0            <<-- after second input
    % 'setosa': 0.0
    % 'versicolor': 0.0
    % 'virginica' : 1.0            <<-- after third input
    ****************************** <<-- load a new model
    *** Initializing model ... ***
    *** Active Fields of Model ***
    *** Field Name: Sepal.Length
    *** Field Name: Sepal.Width
    *** Field Name: Petal.Length
    *** Field Name: Petal.Width
    ******************************
    % 'setosa': 0.0                <<-- after fourth input, using "new" model
    % 'versicolor': 1.0
    % 'virginica' : 0.0
    
    



  