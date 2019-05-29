/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package SPYDER.reconciliation.processors.encodings;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"encoding", "delimiter", "reconciliation","data agenda"})
@CapabilityDescription("Auto Discovers the delimter used in a text/plain MIME-Type file")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="absolute.path", description=""), @ReadsAttribute(attribute="filename", description=""), @ReadsAttribute(attribute="filepath", description="")})
@WritesAttributes({@WritesAttribute(attribute="autodescover.delimiter", description="The delimiter that this processor recommends using")})
public class Plain_Text_Delimiter extends AbstractProcessor {

    public static final PropertyDescriptor MIMETYPE = new PropertyDescriptor.Builder()
        .name("MIME Type")
        .description("The MIME Type of the file (i.e. text/plain)")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("text/plain")
        .required(true)
        .build();
    
    public static final PropertyDescriptor LOCATION = new PropertyDescriptor.Builder()
        .name("File Location")
        .description("The location of the file (i.e. text/plain)")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${absolute.path}/${filename}")
        .required(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("successfully determined a delimiter")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("see attribute \"delimiter.failure.reason\" for failure reason")
        .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MIMETYPE);
        descriptors.add(LOCATION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    HashMap<String, Integer> quoted_delimiter = new HashMap<String, Integer>() {
        {
            put("\\t", 0);
            put(",", 0);
            put(";", 0);
            put("|", 0);
            put("^", 0);
            put("~", 0);
        }
    };



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        try{
            flowFile = session.putAttribute(flowFile, "delimiter.status", "State: Initialised session (ontrigger)");
            flowFile = session.putAttribute(flowFile, "delimiter.filepath", context.getProperty(LOCATION).evaluateAttributeExpressions(flowFile).getValue());

            // int LINE_COUNT;
            // int temp_linecount = 0;
            // try {
            //     // get the amount of lines in the file

            String compiled_path = context.getProperty("absolute.path").evaluateAttributeExpressions(flowFile).getValue() + "/" + context.getProperty("filename").evaluateAttributeExpressions(flowFile).getValue();

            Path path = Paths.get(context.getProperty(LOCATION).evaluateAttributeExpressions(flowFile).getValue());
            flowFile = session.putAttribute(flowFile, "delimiter.status", "State: Created the Path");
            
            
            
            int temp_linecount = (int) (Files.lines(path).count());
            flowFile = session.putAttribute(flowFile, "delimiter.status", "State: Counted Lines :: " + temp_linecount);

            // } catch (Exception e) {
            //     temp_linecount = 0;
            //     flowFile = session.putAttribute(flowFile, "delimiter.error", "Error: " + e.toString());
            //     session.transfer(flowFile, REL_FAILURE);
            // }

            final int LINE_COUNT = temp_linecount;

            // initialise the hashmap for frequency (requires a count of lines)
            HashMap<String, int[]> frequency_delimiter = new HashMap<String, int[]>() {
                {
                    put("\\t", new int[LINE_COUNT]);
                    put(",", new int[LINE_COUNT]);
                    put(";", new int[LINE_COUNT]);
                    put("|", new int[LINE_COUNT]);
                    put("^", new int[LINE_COUNT]);
                    put("~", new int[LINE_COUNT]);
                }
            };


            try {
                int lineNo = 0;
                BufferedReader reader;
                try {

                    // create a buffer reader to hold the file
                    reader = new BufferedReader(new FileReader(context.getProperty(LOCATION).evaluateAttributeExpressions(flowFile).getValue()));

                    flowFile = session.putAttribute(flowFile, "delimiter.status", "State: Loaded File");


                    // read each line to @param::line and loop through each line in the file
                    String line = reader.readLine();

                    flowFile = session.putAttribute(flowFile, "delimiter.status", "State: Read the first line");

                    while (line != null) {
                        System.out.println(line);
                        
                        // for each potential (and supported delimeter) attempt to get the quoted count and frequency for each line
                        for (String potential_delimiter:frequency_delimiter.keySet()) 
                            { 
                                // look for quoted values and build count in 
                                System.out.println(potential_delimiter);

                                int count_quoted = 0;
                                Pattern patterndouble = Pattern.compile(potential_delimiter + "\\s*\".*?\"" + potential_delimiter);
                                Matcher matcherdouble = patterndouble.matcher(line);

                                while (matcherdouble.find())
                                    count_quoted++;

                                Pattern patternsingle = Pattern.compile(potential_delimiter + "\\s*'.*?'" + potential_delimiter);
                                Matcher matchersingle = patternsingle.matcher(line);

                                while (matchersingle.find())
                                    count_quoted++;
                                
                                // append the count to the hashmap
                                quoted_delimiter.put(potential_delimiter, quoted_delimiter.get(potential_delimiter) + count_quoted);

                                
                                // build line frequency in @param::frequency_delimiter
                                int count_frequency = 0;
                                Pattern patternfreq = Pattern.compile(potential_delimiter);
                                Matcher matcherfreq = patternfreq.matcher(line);

                                int countfreq = 0;
                                while (matcherfreq.find())
                                    count_frequency++;

                                int[] temp_array = frequency_delimiter.get(potential_delimiter);
                                temp_array[lineNo] = count_frequency;

                                frequency_delimiter.put(potential_delimiter, temp_array);
                            }

                        line = reader.readLine();
                        lineNo ++;
                    }
                    reader.close();
                } catch (Exception e) {
                    flowFile = session.putAttribute(flowFile, "delimiter.error.2", "Error: " + e.toString());
                    session.transfer(flowFile, REL_FAILURE);
                }

                // Set the default to be a comma
                String pred_delimiter = ",";

                // quote delimited checks
                // 1. More of one type of delimiter

                int max_delimiter_count = 0;
                for (String potential_delimiter:frequency_delimiter.keySet()){
                    if (quoted_delimiter.get(potential_delimiter) > max_delimiter_count){
                        max_delimiter_count = quoted_delimiter.get(potential_delimiter);
                        pred_delimiter = potential_delimiter;
                    }
                }

                if (max_delimiter_count == 0){
                    // frequency analysis checks 
                    // 1. the median
                    // TODO: Implement frequency analysis method
                    flowFile = session.putAttribute(flowFile, "delimiter.status", "State: Did not get a delimiter based on quotations");
                }
                

                flowFile = session.putAttribute(flowFile, "delimiter", pred_delimiter);

                session.transfer(flowFile, REL_SUCCESS);
            }
            catch (Exception e){
                flowFile = session.putAttribute(flowFile, "delimiter.error.1", "Error: " + e.toString());
                session.transfer(flowFile, REL_FAILURE);

            }
        }
        catch (Exception e) {
            flowFile = session.putAttribute(flowFile, "delimiter.error.0", "Error: " + e.toString());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
