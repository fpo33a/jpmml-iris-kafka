/*

Frank Polet
October 2020
Basically refactor a bit https://github.com/hkropp/jpmml-iris-example to understand how work pmml before integrate in a kafka stream application

 */
package jpmmlkafka;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import javax.xml.bind.JAXBException;
import javax.xml.transform.sax.SAXSource;

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

public class JpmmlModelIris {

    private ModelEvaluator<MiningModel> modelEvaluator = null;

    //---------------------------------------------------------------------------------------------

    public JpmmlModelIris() {

    }

    //---------------------------------------------------------------------------------------------

    public static void main(String... args) throws Exception {
        JpmmlModelIris app = new JpmmlModelIris();
        app.initModel();
        app.run();
    }

    //---------------------------------------------------------------------------------------------

    private void initModel() {
        try {
            PMML pmml = createPMMLfromFile("iris_rf.pmml");

            modelEvaluator = new MiningModelEvaluator(pmml);
            printArgumentsOfModel(modelEvaluator);
            modelEvaluator.verify();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //---------------------------------------------------------------------------------------------


    public void run() throws Exception {

        List<String> dataLines = Files.readAllLines(Paths.get(JpmmlModelIris.class.getResource("Iris.csv").toURI()));

        for (String dataLine : dataLines) {
            // System.out.println(dataLine); // (sepal_length,sepal_width,petal_length,petal_width,class)
            if (dataLine.startsWith("sepal_length")) continue;
            System.out.println("--------------");

            Map<FieldName, FieldValue> arguments = getFieldNameAndValueFromCsvData(dataLine, modelEvaluator);
            String category = classifyData(arguments);

            System.out.println("==> Result: " + category);
        }
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

    public Map<FieldName, FieldValue> getFieldNameAndValueFromCsvData(String data, ModelEvaluator<MiningModel> modelEvaluator) {
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

    public void printArgumentsOfModel(ModelEvaluator<MiningModel> modelEvaluator) {
        System.out.println("### Active Fields of Model ####");
        for (FieldName fieldName : modelEvaluator.getActiveFields()) {
            System.out.println("Field Name: " + fieldName);
        }
    }

    //---------------------------------------------------------------------------------------------

    public PMML createPMMLfromFile(String fileName) throws SAXException, JAXBException, FileNotFoundException {
        File pmmlFile = new File(JpmmlModelIris.class.getResource(fileName).getPath());
        String pmmlString = new Scanner(pmmlFile).useDelimiter("\\Z").next();

        InputStream is = new ByteArrayInputStream(pmmlString.getBytes());

        InputSource source = new InputSource(is);
        SAXSource transformedSource = ImportFilter.apply(source);

        return JAXBUtil.unmarshalPMML(transformedSource);
    }

    //---------------------------------------------------------------------------------------------

}
