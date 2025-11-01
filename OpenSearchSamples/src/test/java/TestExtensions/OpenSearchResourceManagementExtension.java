package TestExtensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.*;
import org.opensearch.client.opensearch.OpenSearchClient;

/**
 * JUnit 5 extension to manage OpenSearch resources using a shared Testcontainer.
 * <p>
 * This extension provides the following functionalities:
 * <ul>
 *     <li>Uses a singleton {@link SharedOpenSearchContainer} that starts once for all test classes.</li>
 *     <li>Creates and manages an {@link OpenSearchSharedResource} instance for each test class.</li>
 *     <li>Enables parallel test execution with multiple classes sharing one container.</li>
 *     <li>Resolves parameters annotated in test classes, injecting shared resources like the {@link OpenSearchSharedResource} instance into test constructors or methods.</li>
 * </ul>
 * <p>
 * This extension is optimized for fast integration testing by sharing a single OpenSearch container
 * across all test classes. Each test class should create its own uniquely-named indices to avoid conflicts.
 *
 * <h2>Key Features:</h2>
 * <ul>
 *     <li>Implements {@link BeforeAllCallback} to set up resources for each test class.</li>
 *     <li>Implements {@link AfterAllCallback} for test class cleanup (container remains running).</li>
 *     <li>Implements {@link ParameterResolver} to inject shared test dependencies.</li>
 *     <li>Uses {@link SharedOpenSearchContainer} singleton for optimal performance.</li>
 *     <li>Container is automatically stopped when JVM exits via shutdown hook.</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * @ExtendWith(OpenSearchResourceManagementExtension.class)
 * public class MyTests {
 *     private final OpenSearchSharedResource sharedResource;
 *
 *     public MyTests(OpenSearchSharedResource sharedResource) {
 *         this.sharedResource = sharedResource;
 *     }
 *
 *     @Test
 *     void test1() {
 *         // Create unique index for this test class
 *         String indexName = "my-tests-" + UUID.randomUUID();
 *         Assertions.assertNotNull(sharedResource.getOpenSearchClient());
 *     }
 * }
 * }</pre>
 *
 * @see OpenSearchClient
 * @see OpenSearchSharedResource
 * @see SharedOpenSearchContainer
 * @see BeforeAllCallback
 * @see AfterAllCallback
 * @see ParameterResolver
 */
public class OpenSearchResourceManagementExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private OpenSearchSharedResource openSearchSharedResource;
    private static final Logger logger = LogManager.getLogger(OpenSearchResourceManagementExtension.class);

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        logger.info("Setting up test class with shared OpenSearch container");
        
        // Get the shared client (container will be started if not already running)
        var openSearchClient = SharedOpenSearchContainer.getClient();
        
        openSearchSharedResource = new OpenSearchSharedResource(openSearchClient);
        logger.info("Test class setup completed - using shared container");
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        logger.info("Test class teardown (container remains running for other tests)");
        // Each test class should clean up its own indices if needed
        // The container will be stopped when all tests complete via the extension lifecycle
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        // Only resolve a specific type, not generic Strings
        return parameterContext.getParameter().getType() == OpenSearchSharedResource.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return openSearchSharedResource;
    }
}
