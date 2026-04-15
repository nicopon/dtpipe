#!/bin/bash

# generate_massive_xml.sh
# Usage: ./generate_massive_xml.sh [output_path] [target_gb]

OUTPUT_FILE=${1:-massive.xml}
TARGET_GB=${2:-2}
CHUNK_FILE="temp_chunk.xml"

echo "Generating ~${TARGET_GB}GB XML file at ${OUTPUT_FILE}..."

# 1. Create a 1MB chunk of XML records
echo "" > "$CHUNK_FILE"
for i in {1..2500}; do
  cat <<EOF >> "$CHUNK_FILE"
  <Record id="REC_$i" type="DATA_NODE" status="ACTIVE">
    <Title>Record Alpha $i</Title>
    <Description>Detailed description for record $i. This text is intentionally long to help reach the target file size efficiently while providing realistic data structures for the XML reader to parse.</Description>
    <Metrics>
      <CpuUsage>0.$((RANDOM % 100))</CpuUsage>
      <MemoryUsage>$((RANDOM % 1024))</MemoryUsage>
    </Metrics>
    <Tags>
      <Tag key="environment">production</Tag>
      <Tag key="region">us-east-1</Tag>
      <Tag key="version">1.2.$((RANDOM % 10))</Tag>
    </Tags>
  </Record>
EOF
done

# 2. Approximate size of one record is ~400 bytes. 2500 records ~ 1MB.
# We need TARGET_GB * 1024 chunks.

echo "<Root>" > "$OUTPUT_FILE"

TOTAL_CHUNKS=$((TARGET_GB * 1024))
for ((c=1; c<=TOTAL_CHUNKS; c++)); do
  if (( c % 100 == 0 )); then
    echo "Progress: $c / $TOTAL_CHUNKS MB..."
  fi
  cat "$CHUNK_FILE" >> "$OUTPUT_FILE"
done

echo "</Root>" >> "$OUTPUT_FILE"

rm "$CHUNK_FILE"
echo "Done. File size: $(du -sh "$OUTPUT_FILE" | cut -f1)"
