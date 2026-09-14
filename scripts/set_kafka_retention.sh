#!/bin/bash
sudo docker cp /dev/stdin linkx-kafka:/tmp/fix.sh <<'EOF'
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --topic "dev.xvigilance.transactions.raw.v2" --alter --add-config "retention.ms=172800000,retention.bytes=10737418240"
EOF
sudo docker exec linkx-kafka bash /tmp/fix.sh
