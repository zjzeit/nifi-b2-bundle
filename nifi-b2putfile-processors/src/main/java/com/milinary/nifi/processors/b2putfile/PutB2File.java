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
package com.milinary.nifi.processors.b2putfile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.contentSources.B2ByteArrayContentSource;
import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2UploadFileRequest;


@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutB2File extends AbstractProcessor {

    ////////////////
    // PROPERTIES //
    ////////////////
    public static final PropertyDescriptor PROP_KEYID = new PropertyDescriptor
            .Builder().name("PROP_KEYID")
            .displayName("Key ID")
            .description("A Backblaze B2 application key ID with writeFiles permissions.")
            //.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();
    
    public static final PropertyDescriptor PROP_APPLICATIONKEY = new PropertyDescriptor
            .Builder().name("PROP_APPLICATIONKEY")
            .displayName("Application Key")
            .description("A Backblaze B2 application secret key associated with the keyID.")
            //.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();
    
    public static final PropertyDescriptor PROP_BUCKETID = new PropertyDescriptor
            .Builder().name("PROP_BUCKETID")
            .displayName("Bucket ID")
            .description("A Backblaze B2 destination bucket identifier")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor PROP_ABSOLUTEPATH = new PropertyDescriptor
            .Builder().name("PROP_ABSOLUTEPATH")
            .displayName("Filename")
            .description("The destination path and filename. This path must not start with a slash. Examples: \"myDirectory\\myFile.txt\" or \"myFile.jpg\" or \"etc\\hosts\"")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .defaultValue("${filename}")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();
    
    public static final PropertyDescriptor PROP_USERAGENT = new PropertyDescriptor
            .Builder().name("PROP_USERAGENT")
            .displayName("User Agent")
            .description("The user-agent to use when performing HTTP requests")
            //.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .defaultValue("nifi")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    // Add all PropertyDescriptor objects to List
    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
    		PROP_KEYID,
    		PROP_APPLICATIONKEY,
    		PROP_BUCKETID,
    		PROP_ABSOLUTEPATH,
    		PROP_USERAGENT
    		));

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
    		.name("Success")
    		.description("Successful upload.")
    		.build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
    		.name("Failure")
    		.description("Error result.")
    		.build();
    
    // Add all Relationship objects to Set
    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
    		REL_SUCCESS,
    		REL_FAILURE
    		)));
    
    protected B2StorageClient b2client;

    /////////////
    // METHODS //
    /////////////
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final String USER_AGENT = "Nifi";
    	b2client = B2StorageClientFactory.createDefaultFactory().create("", "", USER_AGENT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }


    
    // Override the getSupportedDynamicPropertyDescriptor method to allow expression language for dynamic properties.
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }
    
    // When processor is scheduled, ensure we update the b2client
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	final String keyID = context.getProperty(PROP_KEYID).getValue();
		final String applicationKey = context.getProperty(PROP_APPLICATIONKEY).getValue();
		final String userAgent = context.getProperty(PROP_USERAGENT).getValue();
		b2client = B2StorageClientFactory.createDefaultFactory().create(keyID, applicationKey, userAgent);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	
    	// Get the flowfile
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }        
        
        // Variable Properties
		final String bucketID = context.getProperty(PROP_BUCKETID).evaluateAttributeExpressions(flowFile).getValue();
		final String absolutePath = context.getProperty(PROP_ABSOLUTEPATH).evaluateAttributeExpressions(flowFile).getValue();
		
    	InputStream contentInputStream = session.read(flowFile);
    	final byte[] contentBytes = getByteArray(contentInputStream);
    	
    	// Close the input stream
    	try {
			contentInputStream.close();
		} catch (IOException e) {
			getLogger().error(e.getLocalizedMessage());
			session.transfer(flowFile, REL_FAILURE);
		}
        final B2ContentSource source = B2ByteArrayContentSource.build(contentBytes);
        
        B2UploadFileRequest request = B2UploadFileRequest
                .builder(bucketID, absolutePath, B2ContentTypes.B2_AUTO, source)
                .setCustomField("color", "red")
                .setCustomField("1", "a")
                .setCustomField("2", "b")
                .setCustomField("3", "c")
                .setCustomField("4", "d")
                .setCustomField("5", "e")
                .setCustomField("6", "f")
                .setCustomField("7", "g")
                .setCustomField("8", "h")
                .setCustomField("9", "i")
                .setCustomField("10", "j")
                .setCustomField("11", "k")
                .setCustomField("12", "l")
                .build();
        try {
        	// Attempt to upload file. Max filesize: 5 GB.
        	B2FileVersion uploadedFile = b2client.uploadSmallFile(request);
		} catch (B2Exception e) {
			getLogger().error(e.getLocalizedMessage());
			session.transfer(flowFile, REL_FAILURE);
			
		}
            
        session.transfer(flowFile, REL_SUCCESS);
        
        
        
		
		
    }
    
    private byte[] getByteArray(InputStream input) {
    	// https://stackoverflow.com/questions/1264709/convert-inputstream-to-byte-array-in-java
    	ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    	int nRead;
    	byte[] data = new byte[4096];

    	try {
			while ((nRead = input.read(data, 0, data.length)) != -1) {
			  buffer.write(data, 0, nRead);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return buffer.toByteArray();
    	
    }
}







