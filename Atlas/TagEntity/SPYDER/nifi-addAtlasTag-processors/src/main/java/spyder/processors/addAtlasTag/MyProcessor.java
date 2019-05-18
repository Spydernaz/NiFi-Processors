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
import java.util.HashSet;
import java.util.List;
import java.util.Set;



/* Code written by spyder by sampling existing nifi processors */
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.expression.ExpressionLanguageScope;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"MetaData", "Meta Data", "Post", "Atlas"})


@CapabilityDescription("Finds entity in Atlas and associates a Tag to the it")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="metadataTags", description="A JSON structured as per Atlas Classifications for assignment of Tags through the")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})



public class MyProcessor extends AbstractProcessor {

    //@TODO: Make more Properties as required
    static final PropertyDescriptor METADATA = new PropertyDescriptor.Builder()
        .name("MetaData")
        .description("JSON of the metadata needed to be attached")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${metadataTags}")
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
        .name("Atlas Base URL (<PROTOCOL>://<HOST>:<PORT>")
        .description("The URL for the Atlas API (base url) ending ")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${qualifiedname}")
        .required(true)
        .build();

        static final PropertyDescriptor ATLAS_USER = new PropertyDescriptor.Builder()
        .name("Atlas User")
        .description("The Atlas User for setting Metadata")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${qualifiedname}")
        .required(true)
        .build();

        // @TODO: Make Password a sensitve property
        static final PropertyDescriptor ATLAS_PASSWORD = new PropertyDescriptor.Builder()
        .name("Qualified Name")
        .description("The password for Atlas User")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("")
        .required(true)
        .build();

        static final PropertyDescriptor NUM_OF_RETRIES = new PropertyDescriptor.Builder()
        .name("Number of retries")
        .description("The number of retries before failing the flowfile")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("30")
        .required(false)
        .build();

    //@TODO: Make more relationships for non-sucessfull flows
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that successfully tags the JSON MetaData to Atlas Entity referenced in the Attributes")
        .build();
    
    //@TODO: Add all Properties to the this variable via below method
    private List<PropertyDescriptor> descriptors;

    //@TODO: Add all Relationships to the this variable via below method
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(METADATA);
        descriptors.add(QUALIFIED_NAME);
        descriptors.add(ATLAS_URL);
        descriptors.add(ATLAS_USER);
        descriptors.add(ATLAS_PASSWORD);
        descriptors.add(NUM_OF_RETRIES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
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
        // @TODO: Implement the post to atlas here (ensure the loop happens for slower Atals integrations)
    }
}
