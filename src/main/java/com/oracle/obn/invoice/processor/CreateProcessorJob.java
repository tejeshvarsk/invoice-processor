package com.oracle.obn.invoice.processor;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.aidocument.AIServiceDocumentClient;
import com.oracle.bmc.aidocument.model.CreateProcessorJobDetails;
import com.oracle.bmc.aidocument.model.DocumentKeyValueExtractionFeature;
import com.oracle.bmc.aidocument.model.DocumentType;
import com.oracle.bmc.aidocument.model.GeneralProcessorConfig;
import com.oracle.bmc.aidocument.model.InlineDocumentContent;
import com.oracle.bmc.aidocument.model.OutputLocation;
import com.oracle.bmc.aidocument.requests.CreateProcessorJobRequest;
import com.oracle.bmc.aidocument.responses.CreateProcessorJobResponse;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;

public class CreateProcessorJob {

        private static Logger logger = Logger.getLogger(CreateProcessorJob.class.getName());

        public static void main(String[] args) throws Exception {
                ConfigFileReader.ConfigFile configFile = getConfigFile();
                String compartment = configFile.get("compartment");
                String region = configFile.get("region");
                String bucket = "outputs";
                String prefix = "results";
                String invoiceFilePath = args[0] != null ? args[0] : "/Users/krsethur/invoice.pdf";
                JsonObject result = CreateProcessorJob.processInvoice(compartment, region, bucket, prefix,
                                invoiceFilePath);
                logger.info(result.toString());
        }

        public static ConfigFileReader.ConfigFile getConfigFile() {
                ConfigFileReader.ConfigFile configFile = null;
                try {
                        configFile = ConfigFileReader.parse("~/.oci/config", "obn");
                } catch (IOException e) {
                        logger.throwing(CreateProcessorJob.class.getName(), "processInvoice", e);
                }
                return configFile;
        }

        public static AuthenticationDetailsProvider getConfigFileAuthenticationDetailsProvider() {
                return new ConfigFileAuthenticationDetailsProvider(getConfigFile());
        }

        public static JsonObject processInvoice(String compartment, String region, String bucketName, String prefix,
                        String invoiceFilePath) throws Exception {
                logger.entering(CreateProcessorJob.class.getName(), "processInvoice");
                logger.logp(Level.INFO, CreateProcessorJob.class.getName(), "processInvoice",
                                "Parameter : compartment : " + compartment);
                logger.logp(Level.INFO, CreateProcessorJob.class.getName(), "processInvoice",
                                "Parameter : region : " + region);
                logger.logp(Level.INFO, CreateProcessorJob.class.getName(), "processInvoice",
                                "Parameter : bucketName : " + bucketName);
                logger.logp(Level.INFO, CreateProcessorJob.class.getName(), "processInvoice",
                                "Parameter : prefix : " + prefix);

                Path inputFilePath = Paths.get(invoiceFilePath);
                JsonObject result = null;
                Region regionName = Region.fromRegionCode(region);

                // final AuthenticationDetailsProvider provider =
                // getConfigFileAuthenticationDetailsProvider();

                InstancePrincipalsAuthenticationDetailsProvider provider = null;
                try {
                        provider = InstancePrincipalsAuthenticationDetailsProvider.builder().build();
                } catch (Exception e) {
                        logger.throwing(CreateProcessorJob.class.getName(), "processInvoice", e);
                }

                AIServiceDocumentClient client = AIServiceDocumentClient.builder().build(provider);

                ObjectStorage objectStorageClient = ObjectStorageClient.builder().region(regionName)
                                .build(provider);
                GetNamespaceResponse namespaceResponse = objectStorageClient
                                .getNamespace(GetNamespaceRequest.builder().build());
                String namespaceName = namespaceResponse.getValue();
                logger.info("Using namespace : " + namespaceName);

                byte[] bytes = null;
                try {
                        bytes = Files.readAllBytes(inputFilePath);
                } catch (IOException e) {
                        logger.throwing(CreateProcessorJob.class.getName(), "processInvoice", e);
                }
                CreateProcessorJobDetails createProcessorJobDetails = CreateProcessorJobDetails.builder()
                                .inputLocation(InlineDocumentContent.builder().data(bytes).build())
                                .outputLocation(OutputLocation.builder().namespaceName(namespaceName)
                                                .bucketName(bucketName)
                                                .prefix(prefix).build())
                                .compartmentId(compartment)
                                .displayName("demo-invoice-processor")
                                .processorConfig(GeneralProcessorConfig.builder().documentType(DocumentType.Invoice)
                                                .features(List.of(DocumentKeyValueExtractionFeature.builder().build()))
                                                .build())
                                .build();
                CreateProcessorJobRequest createProcessorJobRequest = CreateProcessorJobRequest.builder()
                                .createProcessorJobDetails(createProcessorJobDetails).build();
                CreateProcessorJobResponse createProcessorJobResponse = client
                                .createProcessorJob(createProcessorJobRequest);

                logger.info("createProcessorJobResponse.getProcessorJob().getId "
                                + createProcessorJobResponse.getProcessorJob().getId());
                logger.info("createProcessorJobResponse.getProcessorJob().getPercentComplete "
                                + createProcessorJobResponse.getProcessorJob().getPercentComplete());
                logger.info("createProcessorJobResponse.getProcessorJob().getOutputLocation "
                                + createProcessorJobResponse.getProcessorJob().getOutputLocation());
                logger.info("createProcessorJobResponse.getProcessorJob().getLifecycleState "
                                + createProcessorJobResponse.getProcessorJob().getLifecycleState());

                String objectName = String.format("%s/%s/%s", prefix,
                                createProcessorJobResponse.getProcessorJob().getId(), "_/results/defaultObject.json");
                GetObjectResponse getResponse = objectStorageClient
                                .getObject(GetObjectRequest.builder().namespaceName(namespaceName)
                                                .bucketName(bucketName).objectName(objectName).build());
                try (final InputStream fileStream = getResponse.getInputStream()) {
                        Path storageDir = Files.createTempDirectory("fileupload");
                        Files.copy(fileStream,
                                        storageDir.resolve("output.json"),
                                        StandardCopyOption.REPLACE_EXISTING);
                        String rawJsonString = Files.readString(storageDir.resolve("output.json"));
                        JsonReader jsonReader = Json.createReader(new StringReader(rawJsonString));
                        result = jsonReader.readObject();

                } catch (IOException e) {
                        logger.throwing(CreateProcessorJob.class.getName(), "processInvoice", e);
                }

                client.close();
                objectStorageClient.close();
                logger.exiting(CreateProcessorJob.class.getName(), "processInvoice");
                return result;
        }
}
