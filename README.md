```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        A1[IoT GPS Delivery Stream] -->|Live Events| B1[Apache Kafka Broker]
        A2[E-Commerce Batch Orders] -->|Boto3 / Raw Files| B2[LocalStack S3 Bucket\nBronze Layer]
    end

    subgraph Storage_and_Transform["2. Storage & Transformation (Medallion)"]
        B1 -->|Real-Time Consumer| C1[(SQL Server\nStaging / Silver)]
        B2 -->|Batch Processing| C1
        C1 -->|dbt Run & Test| C2[(SQL Server\nGold Star Schema)]
    end

    subgraph Orchestration["3. Orchestration & Monitoring"]
        AIRFLOW[Apache Airflow DAGs] -.->|Triggers & Orchestrates| B2
        AIRFLOW -.->|Executes Models| C1
    end

    subgraph Serving["4. Serving & Analytics Layer"]
        C1 -->|Fast Metric Polling| D1[Grafana\nReal-Time Delivery Dashboard]
        C2 -->|DirectQuery / Import| D2[Power BI\nBusiness Analytics Reports]
    end

    style B2 fill:#f96,stroke:#333,stroke-width:2px
    style B1 fill:#232f3e,stroke:#fff,stroke-width:2px,color:#fff
    style C2 fill:#0078d4,stroke:#333,stroke-width:2px,color:#fff
    style D1 fill:#f46800,stroke:#333,stroke-width:2px,color:#fff
    style D2 fill:#f2c811,stroke:#333,stroke-width:2px,color:#000
\```
