/*

The purpose of this example is to show how to deploy and run a classication ML model stored in PMML format
It is based on famous iris classification ml model

Model is loaded into an "input" 'model' topic.
Raw data (in csv format - sepal_length,sepal_width,petal_length,petal_width,class) is loaded in 'iris' topic
Result of classification is in "iris-others' or 'iris-virginica' topics, depending on model result
Model can be changed and reloaded dynamically while running

Frank Polet
October 2020
Inspired by https://github.com/hkropp/jpmml-iris-example for the pmml/iris part


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
>Terminer le programme de commandes (O/N) ?
^C
// check that records have been classified correctly

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic iris
5.4,3.9,1.7,0.4,Iris-setosa
6.3,3.3,6.0,2.5,Iris-virginica
5.7,2.8,4.1,1.3,Iris-versicolor
Processed a total of 3 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic iris-others
5.4,3.9,1.7,0.4,Iris-setosa
5.7,2.8,4.1,1.3,Iris-versicolor
Processed a total of 2 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic iris-virginica
6.3,3.3,6.0,2.5,Iris-virginica
Processed a total of 1 messages
Terminer le programme de commandes (O/N) ? o

// execute again ModelPublisher ( simulate new model deployment )

// check if new record still classified after having loaded new model
C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic iris
>6.1,3.0,4.6,1.4,Iris-versicolor
// java program output

SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
****************************** <-- at start loads model
*** Initializing model ... ***
*** Active Fields of Model ***
*** Field Name: Sepal.Length
*** Field Name: Sepal.Width
*** Field Name: Petal.Length
*** Field Name: Petal.Width
******************************
% 'setosa'    : 1.0
% 'versicolor': 0.0
% 'virginica' : 0.0             <-- after first input
% 'setosa'    : 0.0
% 'versicolor': 1.0
% 'virginica' : 0.0             <-- after second input
% 'setosa'    : 0.0
% 'versicolor': 0.0
% 'virginica' : 1.0             <-- after third input
******************************  <-- load a new model
*** Initializing model ... ***
*** Active Fields of Model ***
*** Field Name: Sepal.Length
*** Field Name: Sepal.Width
*** Field Name: Petal.Length
*** Field Name: Petal.Width
******************************
% 'setosa'    : 0.0            <-- after fourth input, using "new" model
% 'versicolor': 1.0
% 'virginica' : 0.0


 */

package jpmmlkafka;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import javax.xml.bind.JAXBException;
import javax.xml.transform.sax.SAXSource;

import org.apache.kafka.streams.KeyValue;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.MiningModelEvaluator;
import org.jpmml.evaluator.ProbabilityClassificationMap;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class JpmmlModelKafka {

    private ModelEvaluator<MiningModel> modelEvaluator = null;

    //---------------------------------------------------------------------------------------------

    public JpmmlModelKafka() {

    }

    //---------------------------------------------------------------------------------------------

    public static void main(String... args) throws Exception {
        JpmmlModelKafka app = new JpmmlModelKafka();
        app.streamSetup();
    }

    //---------------------------------------------------------------------------------------------

    private void streamSetup() {
        Properties streamsConfig = new Properties();
        // The name must be unique on the Kafka cluster
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString() ); //"jpmml-example");
        // Brokers
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // SerDes for key and value
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = buildTopology(streamsConfig, "model", "iris", "iris-virginica", "iris-others");
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    //---------------------------------------------------------------------------------------------
    // filter data based on their category

    private StreamsBuilder buildTopology(Properties streamsConfig, String modelTopic, String inputTopic, String okTopic, String koTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        // model stream - initialize when new record received
        KStream<String, String> model = builder.stream(modelTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> initModel(value) );

        // data stream - classify when new record received
        KStream<String, String> dataStream[] = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .map((key,value) -> classifyInputData (key,value) )
                .branch((key, value) -> testCondition(key, value),
                        (key, value) -> true);
        dataStream[0].to(okTopic, Produced.with(Serdes.String(), Serdes.String()));
        dataStream[1].to(koTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    //---------------------------------------------------------------------------------------------

    private KeyValue<String,String> classifyInputData(String key, String value) {
        Map<FieldName, FieldValue> arguments = getFieldNameAndValueFromCsvData(value, modelEvaluator);
        String category = classifyData(arguments);
        return new KeyValue<>(category,value);
    }

    //---------------------------------------------------------------------------------------------

    private String classifyData(Map<FieldName, FieldValue> data) {
        Map<FieldName, ?> results = modelEvaluator.evaluate(data);

        FieldName targetName = modelEvaluator.getTargetField();
        Object targetValue = results.get(targetName);

        ProbabilityClassificationMap nodeMap = (ProbabilityClassificationMap) targetValue;

        System.out.println("% 'setosa'    : " + nodeMap.getProbability("setosa"));
        System.out.println("% 'versicolor': " + nodeMap.getProbability("versicolor"));
        System.out.println("% 'virginica' : " + nodeMap.getProbability("virginica"));

        if (nodeMap.getResult() != null) return nodeMap.getResult().toString();
        return "n/a";
    }

    //---------------------------------------------------------------------------------------------

    private Map<FieldName, FieldValue> getFieldNameAndValueFromCsvData(String data, ModelEvaluator<MiningModel> modelEvaluator) {
        Map<FieldName, FieldValue> fields = new LinkedHashMap<FieldName, FieldValue>();
        String[] columns = data.split(",");

        if (columns.length != 5) return fields;

        FieldValue sepalLength = modelEvaluator.prepare(new FieldName("Sepal.Length"), columns[0].isEmpty() ? 0 : columns[0]);
        FieldValue sepalWidth  = modelEvaluator.prepare(new FieldName("Sepal.Width"),  columns[1].isEmpty() ? 0 : columns[1]);
        FieldValue petalLength = modelEvaluator.prepare(new FieldName("Petal.Length"), columns[2].isEmpty() ? 0 : columns[2]);
        FieldValue petalWidth  = modelEvaluator.prepare(new FieldName("Petal.Width"),  columns[3].isEmpty() ? 0 : columns[3]);

        fields.put(new FieldName("Sepal.Length"), sepalLength);
        fields.put(new FieldName("Sepal.Width"),  sepalWidth);
        fields.put(new FieldName("Petal.Length"), petalLength);
        fields.put(new FieldName("Petal.Width"),  petalWidth);

        return fields;
    }

    //---------------------------------------------------------------------------------------------
    // filtering condition for target topic ...

    private boolean testCondition(String key, String value) {
        if (key == null) return false;
        return (key.compareToIgnoreCase("virginica") == 0);
    }

    //---------------------------------------------------------------------------------------------

    private  String initModel (String model) {
        try {
            System.out.println ("******************************");
            System.out.println ("*** Initializing model ... ***");
            PMML pmml = createPMMLfromString(model);

            modelEvaluator = new MiningModelEvaluator(pmml);
            printFieldsOfModel(modelEvaluator);
            modelEvaluator.verify();
            System.out.println ("******************************");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return model;
    }

    //---------------------------------------------------------------------------------------------

    public PMML createPMMLfromString(String model) throws SAXException, JAXBException, FileNotFoundException {
        String pmmlString = new Scanner(model).useDelimiter("\\Z").next();
        InputStream is = new ByteArrayInputStream(pmmlString.getBytes());

        InputSource source = new InputSource(is);
        SAXSource transformedSource = ImportFilter.apply(source);

        return JAXBUtil.unmarshalPMML(transformedSource);
    }

    //---------------------------------------------------------------------------------------------

    public void printFieldsOfModel(ModelEvaluator<MiningModel> modelEvaluator) {
        System.out.println("*** Active Fields of Model ***");
        for (FieldName fieldName : modelEvaluator.getActiveFields()) {
            System.out.println("*** Field Name: " + fieldName);
        }
    }

    //---------------------------------------------------------------------------------------------

}
