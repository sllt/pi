export const navigation = [
    {
        title: 'Quick Start Guide',
        desc: "Get started with Pi through our Quick Start Guide. Learn to build scalable applications with easy-to-follow instructions on server setup, database connections, configuration management, and more. Boost your productivity and streamline your development process.",
        links: [
            {
                title: 'Hello Server',
                href: '/docs/quick-start/introduction',
                desc: "Getting started with how to write a server using Pi with basic examples and explanations. Boost your productivity with efficient coding practices and learn to build scalable applications quickly."
            },

            {
                title: 'Configuration',
                href: '/docs/quick-start/configuration',
                desc: "Set up environment variables, manage settings, and streamline your development process."
            },
            {
                title: 'Connecting Redis',
                href: '/docs/quick-start/connecting-redis',
                desc: "Discover how to connect your Pi application to Redis for fast in-memory data storage."
            },
            {
                title: 'Connecting MySQL',
                href: '/docs/quick-start/connecting-mysql',
                desc: "Step-by-step guide on integrating MySQL with your Pi application. With managed database connections and new methods for increasing your productivity."
            },
            {
                title: 'Observability',
                href: '/docs/quick-start/observability',
                desc: "Inbuilt logging, tracing, and metrics to enhance reliability and performance."
            },
            {
                title: 'Adding REST Handlers',
                href: '/docs/quick-start/add-rest-handlers',
                desc: "Fastest way to create CRUD APIs by just providing the entity."
            }
        ],
    },
    {
        title: 'Advanced Guide',
        links: [
            {
                title: "Scheduling Cron Jobs",
                href: "/docs/advanced-guide/using-cron",
                desc: "Learn how to schedule and manage cron jobs in your application for automated tasks and background processes with Pi's CRON job management."
            },
            {
                title: 'Overriding Default',
                href: '/docs/advanced-guide/overriding-default',
                desc: "Understand how to override default configurations and behaviors in Pi to tailor framework to your specific needs."
            },
            {
                title: 'Remote Log Level Change',
                href: '/docs/advanced-guide/remote-log-level-change',
                desc: "Discover how to dynamically change log levels remotely, enabling you to adjust logging verbosity without redeploying your application."
            },
            {
                title: 'Publishing Custom Metrics',
                href: '/docs/advanced-guide/publishing-custom-metrics',
                desc: "Explore methods for publishing custom metrics to monitor your application's performance and gain valuable insights."
            },
            {
                title: 'Custom Headers in Response',
                href: '/docs/advanced-guide/setting-custom-response-headers',
                desc: "Learn how to include custom headers in HTTP responses to provide additional context and control to your API clients."
            },
            {
                title: 'Custom Spans in Tracing',
                href: '/docs/advanced-guide/custom-spans-in-tracing',
                desc: "Learn to create custom spans for tracing to enhance observability and analyze the performance of your services."
            },
            {
                title: 'Adding Custom Middleware',
                href: '/docs/advanced-guide/middlewares',
                desc: "Learn how to add custom middleware to your Pi application for enhanced functionality and request processing."
            },
            {
                title: 'HTTP Communication',
                href: '/docs/advanced-guide/http-communication',
                desc: "Get familiar with making HTTP requests and handling responses within your Pi application to facilitate seamless communication."
            },
            {
                title: 'Authentication',
                href: '/docs/advanced-guide/authentication',
                desc: "Implement various authentication methods to secure your Pi application and protect sensitive endpoints across HTTP and gRPC."
            },
            {
                title: 'Role-Based Access Control (RBAC)',
                href: '/docs/advanced-guide/rbac',
                desc: "Implement comprehensive Role-Based Access Control with support for roles, permissions, hierarchy, JWT integration, and fine-grained permission-based authorization."
            },
            {
                title: 'Circuit Breaker Support',
                href: '/docs/advanced-guide/circuit-breaker',
                desc: "Understand how to implement circuit breaker patterns to enhance the resilience of your services against failures."
            },
            {
                title: 'Monitoring Service Health',
                href: '/docs/advanced-guide/monitoring-service-health',
                desc: "Learn to monitor the health of your services effectively, ensuring optimal performance and quick issue resolution."
            },
            {
                title: 'Handling Data Migrations',
                href: '/docs/advanced-guide/handling-data-migrations',
                desc: "Explore strategies for managing data migrations within your Pi application to ensure smooth transitions and data integrity."
            },
            {
                title: 'Writing gRPC Server/Client',
                href: '/docs/advanced-guide/grpc',
                desc: "Step-by-step guide on writing a gRPC server in Pi to facilitate efficient communication between services."
            },
            {
                title: 'gRPC Streaming',
                href: '/docs/advanced-guide/grpc-streaming',
                desc: "Learn how to implement server-side, client-side, and bidirectional streaming in Pi with built-in observability and error handling."
            },
            {
                title: 'Using Pub/Sub',
                href: '/docs/advanced-guide/using-publisher-subscriber',
                desc: "Discover how to Pi seamlessly allows to integrate different Pub/Sub systems in your application for effective messaging and event-driven architectures."
            },
            {
                title: 'Key Value Store',
                href: '/docs/advanced-guide/key-value-store',
                desc: "Explore how to implement and manage a key-value store in your Pi application for fast and efficient data retrieval. Supports BadgerDB, NATS-KV, and DynamoDB."
            },
            {
                title: 'Dealing with SQL',
                href: '/docs/advanced-guide/dealing-with-sql',
                desc: "Get insights into best practices for working with SQL databases in Pi, including query optimization and error handling."
            },
            {
                title: 'Automatic SwaggerUI Rendering',
                href: '/docs/advanced-guide/swagger-documentation',
                desc: "Learn how to automatically render SwaggerUI documentation for your Pi APIs, improving discoverability and usability."
            },
            {
                title: 'Adding Synchronous Startup Hooks',
                href: '/docs/advanced-guide/startup-hooks',
                desc: "Learn how to seed a database, warm up a cache, or perform other critical setup procedures, synchronously before starting your application."
            },
            {
                title: 'Error Handling',
                href: '/docs/advanced-guide/gofr-errors',
                desc: "Understand error handling mechanisms in Pi to ensure robust applications and improved user experience."
            },
            {
                title: 'Handling File',
                href: '/docs/advanced-guide/handling-file',
                desc: "Explore how Pi enables efficient file handling by abstracting remote and local filestore providers in your Go application. Learn to manage file uploads, downloads, and storage seamlessly, enhancing your application's capability to work with diverse data sources."
            },
            {
                title: 'WebSockets',
                href: '/docs/advanced-guide/websocket',
                desc: "Explore how Pi eases the process of WebSocket communication in your Golang application for real-time data exchange."
            },
            {
                title: 'Serving-Static Files',
                href: '/docs/advanced-guide/serving-static-files',
                desc: "Know how Pi automatically serves static content from a static folder in the application directory."
            },
            {
                title: 'Profiling in Pi Applications',
                href: '/docs/advanced-guide/debugging',
                desc: "Discover Pi auto-enables pprof profiling by leveraging its built-in configurations."
            },
            {
                title: 'Building CLI Applications',
                href: '/docs/advanced-guide/building-cli-applications',
                desc: "Learn to build powerful command-line interface (CLI) applications using Pi's app.NewCMD(), offering a robust framework for command-line tools."
            },
        ],
    },
    {
        title: 'Datasources',
        links: [
            {
                title: "Getting Started",
                href: "/docs/datasources/getting-started",
                desc: "Learn how to connect to and interact with multiple databases in Pi."
            },
            {
                title: "ArangoDB",
                href: "/docs/datasources/arangodb",
                desc: "Learn how to connect to and interact with arango database in Pi."
            },
            {
                title: "Cassandra",
                href: "/docs/datasources/cassandra",
                desc: "Learn how to connect to and interact with cassandra database in Pi."
            },
            {
                title: "ClickHouse",
                href: "/docs/datasources/clickhouse",
                desc: "Learn how to connect to and interact with clickhouse database in Pi."
            },
            {
                title: "CockroachDB",
                href: "/docs/datasources/cockroachdb",
                desc: "Learn how to connect to and interact with CockroachDB in Pi."
            },
            {
                title: "Couchbase",
                href: "/docs/datasources/couchbase",
                desc: "Learn how to connect to and interact with couchbase database in Pi."
            },
            {
                title: "DGraph",
                href: "/docs/datasources/dgraph",
                desc: "Learn how to connect to and interact with dgraph database in Pi."
            },
            {
                title: "MongoDB",
                href: "/docs/datasources/mongodb",
                desc: "Learn how to connect to and interact with mongo database in Pi."
            },
            {
                title: "OpenTSDB",
                href: "/docs/datasources/opentsdb",
                desc: "Learn how to connect to and interact with opentsdb database in Pi."
            },
            {
                title: "OracleDB",
                href: "/docs/datasources/oracle",
                desc: "Learn how to connect to and interact with oracle database in Pi."
            },
            {
                title: "ScyllaDB",
                href: "/docs/datasources/scylladb",
                desc: "Learn how to connect to and interact with scylla database in Pi."
            },
            {
                title: "Solr",
                href: "/docs/datasources/solr",
                desc: "Learn how to connect to and interact with solr database in Pi."
            },
            {
                title: "SurrealDB",
                href: "/docs/datasources/surrealdb",
                desc: "Learn how to connect to and interact with surreal database in Pi."
            },
            {
                title: "Elasticsearch",
                href: "/docs/datasources/elasticsearch",
                desc: "Learn how to connect to and interact with elasticsearch in Pi."
            },
            {
                title: "InfluxDB",
                href: "/docs/datasources/influxdb",
                desc: "Learn how to connect to and interact with influxdb in Pi."
            },
        ],
    },
    {
        title: 'References',
        links: [
            {
                title: 'Context',
                href: '/docs/references/context',
                desc: "Discover the Pi context, an injected object that simplifies request-specific data handling for HTTP, gRPC, and Pub/Sub calls. Learn how it extends Go's context, providing easy access to dependencies like databases, loggers, and HTTP clients. Explore features for reading HTTP requests, binding data, and accessing query and path parameters efficiently, all while reducing application complexity."
            },
            {
                title: 'Configs',
                href: '/docs/references/configs',
                desc: "Learn how to manage configuration settings in your Pi applications, including default values for environment variables. This section provides a comprehensive list of all available configurations to streamline your setup."
            },
            {
                title: 'Testing',
                href: '/docs/references/testing',
                desc: "Pi provides a centralized collection of mocks to facilitate writing effective unit tests. Explore testing strategies and tools for Pi applications, ensuring the code is robust, reliable, and maintainable."
            },

            {
                title: 'Pi CLI',
                href: '/docs/references/gofrcli',
                desc: "Pi CLI is the command line tool for initializing projects and performing tasks in accordance with Pi framework."
            }
        ],
    },
]