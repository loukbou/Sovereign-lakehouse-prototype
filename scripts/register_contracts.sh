#!/bin/bash
# Registers all data contracts and Avro schemas in Apicurio Registry.
# Automatically discovers all files in the directories.

APICURIO_URL=${APICURIO_URL:-http://localhost:8080}

register_contract() {
  GROUP=$1
  ARTIFACT_ID=$2
  FILE=$3

  echo "--> Registering contract: $GROUP/$ARTIFACT_ID from $FILE"

  CONTENT=$(python3 -c "
import json
with open('$FILE') as f:
    data = json.load(f)
print(json.dumps(json.dumps(data)))
")

  curl -s -X POST "$APICURIO_URL/apis/registry/v3/groups/$GROUP/artifacts?ifExists=FIND_OR_CREATE_VERSION" \
    -H "Content-Type: application/json" \
    -d "{
      \"artifactId\": \"$ARTIFACT_ID\",
      \"artifactType\": \"JSON\",
      \"firstVersion\": {
        \"version\": \"1.0.0\",
        \"content\": {
          \"content\": $CONTENT,
          \"contentType\": \"application/json\"
        }
      }
    }" | python3 -c "
import json,sys
r=json.load(sys.stdin)
if 'artifactId' in r.get('artifact',{}):
    version = r.get('version', 'unknown')
    print(f'   OK (created or found) - version: {version}')
else:
    print('   ERROR:', json.dumps(r))
"
}

register_avro() {
  GROUP=$1
  ARTIFACT_ID=$2
  FILE=$3

  echo "--> Registering Avro schema: $GROUP/$ARTIFACT_ID from $FILE"

  CONTENT=$(python3 -c "
import json
with open('$FILE') as f:
    data = json.load(f)
print(json.dumps(json.dumps(data)))
")

  curl -s -X POST "$APICURIO_URL/apis/registry/v3/groups/$GROUP/artifacts?ifExists=FIND_OR_CREATE_VERSION" \
    -H "Content-Type: application/json" \
    -d "{
      \"artifactId\": \"$ARTIFACT_ID\",
      \"artifactType\": \"AVRO\",
      \"firstVersion\": {
        \"version\": \"1.0.0\",
        \"content\": {
          \"content\": $CONTENT,
          \"contentType\": \"application/json\"
        }
      }
    }" | python3 -c "
import json,sys
r=json.load(sys.stdin)
if 'artifactId' in r.get('artifact',{}):
    version = r.get('version', 'unknown')
    print(f'   OK (created or found) - version: {version}')
else:
    print('   ERROR:', json.dumps(r))
"
}

echo "=== Registering Avro Schemas ==="

# --- Explicit overrides ---
# sensor_alert.avsc and sensors_alerts.json produce ambiguous artifact IDs
# under the generic naming rule below, which caused for_topic("alerts") in
# contract_engine.py to resolve to the wrong artifact. Pin these explicitly.
if [ -f "schemas/sensor_alert.avsc" ]; then
  register_avro "sensors" "alerts-schema" "schemas/sensor_alert.avsc"
fi
if [ -f "data_contracts/sensors_alerts.json" ]; then
  register_contract "sensors" "alerts" "data_contracts/sensors_alerts.json"
fi

# Register remaining .avsc files in schemas directory (auto-discovered)
for file in schemas/*.avsc; do
  if [ -f "$file" ] && [ "$file" != "schemas/sensor_alert.avsc" ]; then
    # Extract filename without extension
    filename=$(basename "$file" .avsc)
    # Replace underscores with hyphens for artifact ID
    artifact_id=$(echo "$filename" | tr '_' '-')
    register_avro "sensors" "$artifact_id" "$file"
  fi
done

echo ""
echo "=== Registering Data Contracts ==="
# Register remaining .json files in data_contracts directory (auto-discovered)
for file in data_contracts/*.json; do
  if [ -f "$file" ] && [ "$file" != "data_contracts/sensors_alerts.json" ]; then
    # Extract filename without extension
    filename=$(basename "$file" .json)
    # Replace underscores with hyphens for artifact ID
    artifact_id=$(echo "$filename" | tr '_' '-')
    register_contract "sensors" "$artifact_id" "$file"
  fi
done

echo ""
echo "=== Done. Verify at: $APICURIO_URL/ui ==="