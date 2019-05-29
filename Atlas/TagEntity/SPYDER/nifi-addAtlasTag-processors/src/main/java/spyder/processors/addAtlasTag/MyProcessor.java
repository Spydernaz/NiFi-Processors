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
package spyder.processors.addAtlasTag;

import org.apache.nifi.components.PropertyDescriptor;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



/* Code written by spyder by sampling existing nifi processors */
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.expression.ExpressionLanguageScope;

/* Import for web requests */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;



/* Imports for JSON handling */
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;


@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"MetaData", "Meta Data", "Post", "Atlas"})


@CapabilityDescription("Finds entity in Atlas and associates a Tag to the it")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="metadataTags", description="A JSON structured as per Atlas Classifications for assignment of Tags through the")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})



public class MyProcessor extends AbstractProcessor {

    private final String USER_AGENT = "Mozilla/5.0";


    //@TODO: Make more Properties as required
    static final PropertyDescriptor METADATA = new PropertyDescriptor.Builder()
        .name("MetaData")
        .description("JSON of the metadata needed to be attached")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${metadataTags}")
        .required(true)
        .build();

        static final PropertyDescriptor ENTITY_TYPE = new PropertyDescriptor.Builder()
        .name("Entity Type")
        .description("The Qualified Type of Entity (i.e. \"hdfs_path\"")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("hdfs_path")
        .required(true)
        .build();

        static final PropertyDescriptor QUALIFIED_NAME = new PropertyDescriptor.Builder()
        .name("Qualified Name")
        .description("The Qualified Name of the Entity wanting to tag")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${qualifiedname}")
        .required(true)
        .build();

        static final PropertyDescriptor ATLAS_URL = new PropertyDescriptor.Builder()
        .name("Atlas Base URL")
        .description("The URL for the Atlas API (base url) formatted as  <PROTOCOL>://<HOST>:<PORT>")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("http://atlas:21000")
        .required(true)
        .build();

        static final PropertyDescriptor ATLAS_USER = new PropertyDescriptor.Builder()
        .name("Atlas User")
        .description("The Atlas User for setting Metadata")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("admin")
        .required(true)
        .build();

        // @TODO: Make Password a sensitve property
        static final PropertyDescriptor ATLAS_PASSWORD = new PropertyDescriptor.Builder()
        .name("Atlas Password")
        .description("The password for Atlas User")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("admin")
        .required(true)
        .build();

        // static final PropertyDescriptor NUM_OF_RETRIES = new PropertyDescriptor.Builder()
        // .name("Number of retries")
        // .description("The number of retries before failing the flowfile")
        // .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        // .defaultValue("30")
        // .required(false)
        // .build();

    //@TODO: Make more relationships for non-sucessfull flows
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that successfully tags the JSON MetaData to Atlas Entity referenced in the Attributes")
        .build();

        //@TODO: Needs to be more verbose failures
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that fails")
        .build();
    
    private List<PropertyDescriptor> descriptors;

    //@TODO: Add all Relationships to the this variable via below method
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(METADATA);
        descriptors.add(QUALIFIED_NAME);
        descriptors.add(ENTITY_TYPE);
        descriptors.add(ATLAS_URL);
        descriptors.add(ATLAS_USER);
        descriptors.add(ATLAS_PASSWORD);
        // descriptors.add(NUM_OF_RETRIES);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        // @TODO: Get the GUID of an entity and set the Attribute
        try {

            String creds = "" + context.getProperty(ATLAS_USER).getValue() + ":" + context.getProperty(ATLAS_PASSWORD).getValue() + "";
            String authStr = Base64.getEncoder().encodeToString(creds.getBytes());
            String baseurl = "http://" + context.getProperty(ATLAS_URL).getValue();

            flowFile = session.putAttribute(flowFile, "baseurl", baseurl);

            URL obj = new URL(baseurl + "/api/atlas/v2/entity/uniqueAttribute/type/" + context.getProperty(ENTITY_TYPE).getValue() + "?attr:qualifiedName=" + context.getProperty(QUALIFIED_NAME).evaluateAttributeExpressions().getValue());
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setRequestProperty("Authorization", "Basic " + authStr);
            Map<String, List<String>> headers = con.getHeaderFields();

            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", USER_AGENT);
            int responseCode = con.getResponseCode();
            System.out.println("GET Response Code :: " + responseCode);
            if (responseCode == HttpURLConnection.HTTP_OK) { // success
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                // Get GUID

                flowFile = session.putAttribute(flowFile, "api_response", response.toString());

                System.out.println(response.toString());
                JSONObject qnjson = new JSONObject(response.toString());
                String guid = qnjson.getString("guid");

                flowFile = session.putAttribute(flowFile, "guid", guid);

                // Get Tag to the GUID
                String guidurl = "http://" + context.getProperty(ATLAS_URL).evaluateAttributeExpressions().getValue() + "/api/atlas/v2/entity/guid/" + guid + "/classifications";
                URL guidobj = new URL(guidurl);
                HttpURLConnection guidcon = (HttpURLConnection) guidobj.openConnection();
                guidcon.setRequestMethod("GET");
                guidcon.setRequestProperty("User-Agent", USER_AGENT);
                int guidresponseCode = guidcon.getResponseCode();
                System.out.println("GET Response Code :: " + guidresponseCode);
                if (guidresponseCode == HttpURLConnection.HTTP_OK) { // success
                    BufferedReader guidin = new BufferedReader(new InputStreamReader(
                            guidcon.getInputStream()));
                    String guidinputLine;
                    StringBuffer guidresponse = new StringBuffer();

                    while ((guidinputLine = guidin.readLine()) != null) {
                        guidresponse.append(guidinputLine);
                    }
                    in.close();

                    // print result
                    System.out.println(guidresponse.toString());
                    JSONObject guidjson = new JSONObject(guidresponse.toString());
                    JSONArray guidclassifications = new JSONArray(guidjson.getString("list"));

                    flowFile = session.putAttribute(flowFile, "Classifications", guidclassifications.getString(0));

                } else {
                    System.out.println("cannot get classifications");
                }

            } else {
                System.out.println("GET request not worked");
                session.transfer(flowFile, REL_FAILURE);

            }

            session.transfer(flowFile, REL_SUCCESS);

        }


        catch (Exception e){
            flowFile = session.putAttribute(flowFile, "Classifications", "Error: " + e.toString());
            session.transfer(flowFile, REL_FAILURE);
        }
        finally {
            //do something?

        }

        // @TODO: Implement the post to atlas here (ensure the loop happens for slower Atals integrations)
    }
}
