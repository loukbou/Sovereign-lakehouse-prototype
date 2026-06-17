"""

Automated Table Provisioning Script.
Reads data contracts, maps contract types to native Iceberg SQL data types,
and explicitly ensures both the Main Table and Quarantine Table exist with
the proper schema drift catch-all structures before streaming begins.
"""

import os
import sys
from pyspark.sql import SparkSession
# Re-uses the optimized ContractEngine class from contract_engine.py
from contract_engine import ContractEngine

# Type mapping dictionary: Translates contract schema types to Iceberg SQL types
TYPE_MAP = {
    "string": "STRING",
    "int": "INT",
    "integer": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "bytes": "BINARY"
}

    
def generate_iceberg_ddl(engine: ContractEngine) -> tuple:
    """
    Generates DDL statements for both the Main and Quarantine Iceberg tables.
    Includes systemic operational audit fields and a schema drift catch-all map.
    """
    
    # 1. Build the Main Table Column Definition from Contract Fields
    columns_spec = []
    for field_name, field_def in engine._fields.items():
        contract_type = field_def.get("type", "string").lower()
        spark_type = TYPE_MAP.get(contract_type, "STRING")
        nullable = " " if field_def.get("nullable", True) else " NOT NULL"
        columns_spec.append(f"  {field_name} {spark_type}{nullable}")
        
    # Append operational audit columns used by your streaming engine
    columns_spec.append("  ingested_at TIMESTAMP")
    columns_spec.append("  schema_version INT")
    columns_spec.append("  is_valid BOOLEAN")
    columns_spec.append("  validation_errors ARRAY<STRING>")
    
    # CRITICAL: Schema Drift Catch-All Column. 
    # Holds any unmapped keys sent by upstream systems in a JSON map.
    columns_spec.append("  additional_attributes MAP<STRING, STRING>")
    partition_clause = ""

    if engine.partitioning:

        partitions = ",\n".join(engine.partitioning)

        partition_clause = f"""
    PARTITIONED BY (
    {partitions}
    )
    """
    main_fields_sql = ",\n".join(columns_spec)
    
    main_ddl = f"""
    CREATE TABLE IF NOT EXISTS {engine.iceberg_table} (
    {main_fields_sql}
    ) 
    USING iceberg
    TBLPROPERTIES (
      'write.format.default'='parquet',
      'history.expire.max-snapshot-age-ms'='604800000', -- Auto-expire snapshots after 7 days
      'write.spark.accept-any-schema'='true'           -- Permits seamless Iceberg schema evolution
    )
    """
    
    # 2. Build the Quarantine Table Definition
    quarantine_ddl = f"""
    CREATE TABLE IF NOT EXISTS {engine.quarantine_table} (
      raw_payload STRING,
      ingested_at TIMESTAMP,
      schema_version INT,
      validation_errors ARRAY<STRING>
    ) 
    USING iceberg
    TBLPROPERTIES (
      'write.format.default'='parquet'
    )
    """
    
    return main_ddl.strip(), quarantine_ddl.strip()
def provision_all_contracts(kafka_topic: str = None):
    """
    Scans data contracts, compiles explicit schemas, and issues
    ACID-compliant table creation DDL commands via the active catalog.
    """
    
    spark = (SparkSession.builder
             .appName("iceberg-table-provisioner")
             # Inject the default catalog mappings explicitly so raw execution works
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.defaultCatalog", "nessie")
             .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
             .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2")
             .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
             .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             # Target your VM Ceph Endpoint
             .config("spark.sql.catalog.nessie.s3.endpoint", "http://192.168.122.246:80")
             .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
             .config("spark.sql.catalog.nessie.s3.access-key-id", "lakehouse")
             .config("spark.sql.catalog.nessie.s3.secret-access-key", "lakehouse123")
             .config("spark.sql.catalog.nessie.s3.region", "lakehouse-zg")
             .getOrCreate())
    
    print("============ Starting Lakehouse Table Provisioning Automation...")
    
    try:
        if kafka_topic:
            engines = [ContractEngine.for_topic(kafka_topic)]
        else:
            # Discover all registered topics from Apicurio.
            # Filter to artifactType=JSON only: AVRO entries are schemas
            # (e.g. "alerts-schema"), not data contracts, and have no
            # kafkaTopic field, which previously produced an empty
            # "CREATE NAMESPACE IF NOT EXISTS ." statement.
            import requests
            base = f"{os.environ.get('APICURIO_URL', 'http://apicurio:8080')}/apis/registry/v3"
            resp = requests.get(
                f"{base}/search/artifacts",
                params={"limit": 100, "artifactType": "JSON"},
                timeout=10,
            )
            resp.raise_for_status()
            artifacts = resp.json().get("artifacts", [])
            engines = []
            for a in artifacts:
                group = a["groupId"]
                artifact = a["artifactId"]
                # Reconstruct topic: bronze.<group>.<artifact>
                topic = f"bronze.{group}.{artifact}"
                try:
                    engines.append(ContractEngine.for_topic(topic))
                except Exception as e:
                    print(f"⚠️ Skipping {group}/{artifact}: {e}")
    except Exception as e:
        print(f"❌ Error loading contract files: {e}")
        sys.exit(1)

    if not engines:
        print(f"⚠️ No contracts found ")
        spark.stop()
        return

    for engine in engines:
        print(f"\nEvaluating Contract: '{engine.name}' for topic '{engine.kafka_topic}'")
        main_sql, quarantine_sql = generate_iceberg_ddl(engine)
        
        parts = engine.iceberg_table.split(".")

        if len(parts) >= 4:
            catalog = parts[0]          # nessie
            zone = parts[1]             # bronze
            namespace = parts[2]        # sensors
            table_name = parts[3]       # alerts
            
            # Chemin complet du sous-namespace sous la zone (ex: nessie.bronze.sensors)
            full_namespace = f"{catalog}.{zone}.{namespace}"
            
            print(f"📁 Creating hierarchical namespace '{full_namespace}'...")
            # 1. On crée la zone racine (ex: nessie.bronze)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{zone}")
            # 2. On crée le sous-namespace (ex: nessie.bronze.sensors)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}")
            
        else:
            # Fallback de secours si tu as oublié de mettre la zone dans un autre contrat
            catalog = parts[0]
            namespace = ".".join(parts[1:-1])
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

        # Execute Main Table Creation
        print(f"🔨 Ensuring Main Iceberg Table exists: {engine.iceberg_table}")
        spark.sql(main_sql)
        
        # Execute Quarantine Table Creation
        print(f"🔨 Ensuring Quarantine Iceberg Table exists: {engine.quarantine_table}")
        spark.sql(quarantine_sql)

    print("\n✅ Infrastructure provisioning complete. Spark can safely write to tables.")
    spark.stop()

if __name__ == "__main__":
    KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", None)
    provision_all_contracts(KAFKA_TOPIC)