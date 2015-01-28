/*
 * Copyright 2015 Codice Foundation
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.codice.testify.processors.ldap;

import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPResult;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFReader;
import org.codice.testify.objects.TestifyLogger;
import org.codice.testify.objects.Request;
import org.codice.testify.objects.Response;
import org.codice.testify.processors.TestProcessor;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * The LdapTestProcessor is a Testify TestProcessor service for adding ldif files to an LDAP server
 */
public class LdapTestProcessor implements BundleActivator, TestProcessor {

    private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    private DocumentBuilder builder = null;
    private Document document = null;

    @Override
    public Response executeTest(Request request) {
        TestifyLogger.debug("Running " + this.getClass().getSimpleName(), this.getClass().getSimpleName());

        // Set up document builder to extract xml elements
        try {
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) { TestifyLogger.error("Error creating document builder " + e, this.getClass().getSimpleName()); }

        try {
            document = builder.parse(new InputSource(new StringReader("<test>" + request.getTestBlock() + "</test>")));
        } catch (SAXException e) { TestifyLogger.error("Error parsing xml input " + e, this.getClass().getSimpleName());
        } catch (IOException e) { TestifyLogger.error("Error reading xml input " + e, this.getClass().getSimpleName()); }

        // Extract each element from test block
        Element rootElement = document.getDocumentElement();

        // Required Elements
        String bindDn = getString("bindDn", rootElement);
        String password = getString("password", rootElement);
        String file = getString("file", rootElement);

        LDIFReader ldifReader = null;
        String resultString = "LDAP Result: ";

        try{
            //Load Ldif File
            if (file != null) {
                TestifyLogger.info("File: " + file, this.getClass().getSimpleName());
                try {
                    ldifReader = new LDIFReader(file);
                } catch (IOException e) {
                    TestifyLogger.error("Error reading ldif file: " + file + " " + e.getMessage(), this.getClass().getSimpleName());
                }
            }

            // Create LDAP Connection
            String[] endpoint = request.getEndpoint().split(":");
            String host = endpoint[0];
            int port = Integer.parseInt(endpoint[1]);
            LDAPConnection connection = null;
            try {
                connection = new LDAPConnection( host, port );
            } catch (LDAPException e) {
                TestifyLogger.error("Error occurred while attempting to create connection object: " + e.getMessage(), this.getClass().getSimpleName());
            }
            
            //Attempt to bind using the bindDn and password
            if (bindDn != null && password != null) {
                TestifyLogger.info("Bind DN: " + bindDn, this.getClass().getSimpleName());
                TestifyLogger.info("Password: " + password, this.getClass().getSimpleName());
                try {
                    connection.bind(bindDn, password);
                } catch (LDAPException e) {
                    TestifyLogger.error("Error occurred while attempting to bind to the ldap. " + e.getMessage(), this.getClass().getSimpleName());
                }
            }

            //The following is based on "http://stackoverflow.com/questions/21141106/ldif-bulk-import-programatically"
            int entriesRead = 0;
            int entriesAdded = 0;
            int errorsEncountered = 0;
            Entry entry;
            
            //Keep running this loop until something inside the loop breaks this
            while (true)
            {
                try
                {
                    entry = ldifReader.readEntry();
                    
                    //If the entry is null meaning that everything has been read, break the loop
                    if (entry == null)
                    {
                        TestifyLogger.debug("All ldif entries have been read", this.getClass().getSimpleName());
                        break;
                    }
                    entriesRead++;
                }
                catch (LDIFException le)
                {
                    errorsEncountered++;
                    if (le.mayContinueReading())
                    {
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }
                catch (IOException e)
                {
                    TestifyLogger.error("An error occurred while attempting to read from the LDIF file. No further LDIF processing will be performed: " + e.getMessage(), this.getClass().getSimpleName());
                    errorsEncountered++;
                    break;
                }

                TestifyLogger.debug(entry.toLDIFString(), this.getClass().getSimpleName());
                
                //Add the returned entry to the result string
                try
                {
                    LDAPResult addResult = connection.add(entry);
                    TestifyLogger.debug(addResult.toString(), this.getClass().getSimpleName());
                    resultString = resultString + addResult.toString() + " ;; ";
                    entriesAdded++;
                }
                catch (LDAPException e)
                {
                    TestifyLogger.error("An error occurred while attempting to add an entry: " + e.getMessage(), this.getClass().getSimpleName());
                    resultString = resultString + e.getMessage() + " ;; ";
                    errorsEncountered++;
                }
            }
            
        } finally {
            
            //Close out the reader
            try {
                ldifReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return new Response(resultString);
    }

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        
        //Register the LdapTestProcessor service
        bundleContext.registerService(TestProcessor.class.getName(), new LdapTestProcessor(), null);
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {

    }

    protected String getString(String tagName, Element element) {
        NodeList list = element.getElementsByTagName(tagName);
        if (list != null && list.getLength() > 0) {
            NodeList subList = list.item(0).getChildNodes();

            if (subList != null && subList.getLength() > 0) {
                return subList.item(0).getNodeValue();
            }
        }

        return null;
    }
}