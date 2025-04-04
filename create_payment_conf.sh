#!/bin/bash
# Create the tmp_payment_conf directory if it doesn't exist
mkdir -p /tmp_payment_conf

# Write the sentinel configuration to the file
cat <<EOF > /tmp_payment_conf/sentinel-payment.conf
sentinel resolve-hostnames yes
sentinel monitor mymaster payment-db 6379 1
sentinel auth-pass mymaster redis
sentinel down-after-milliseconds mymaster 5000
sentinel parallel-syncs mymaster 1
EOF

# Now start the Redis server using the config
exec redis-server /tmp_payment_conf/sentinel-payment.conf --sentinel
