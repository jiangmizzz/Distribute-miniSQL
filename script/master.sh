#!/bin/bash
systemctl stop etcd > /dev/null

if [ "$#" -ne 1 ]; then
    echo "请传入[本机公网ip]"
    exit 1
fi

tar -vxf etcd-v3.5.13-linux-amd64.tar.gz
mv etcd-v3.5.13-linux-amd64/etcd* /usr/local/bin/
rm /var/lib/etcd/member -rf
mkdir -p /var/lib/etcd/
mkdir -p /etc/etcd

IP=$1

tee /etc/etcd/etcd.conf > /dev/null << EOF
ETCD_NAME=etcd-$IP
ETCD_DATA_DIR="/var/lib/etcd"
ETCD_LISTEN_PEER_URLS="http://0.0.0.0:2380"
ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379"
ETCD_ADVERTISE_CLIENT_URLS="http://$IP:2379"
ETCD_INITIAL_ADVERTISE_PEER_URLS="http://$IP:2380"
ETCD_INITIAL_CLUSTER_TOKEN="etcd-cluster"
ETCD_INITIAL_CLUSTER="etcd-$IP=http://$IP:2380"
ETCD_INITIAL_CLUSTER_STATE="new"
EOF

tee /usr/lib/systemd/system/etcd.service > /dev/null << EOF
[Unit]
Description=Etcd Server
After=network.target
After=network-online.target
Wants=network-online.target

[Service]
User=root
Type=notify
EnvironmentFile=-/etc/etcd/etcd.conf
ExecStart=/usr/local/bin/etcd
Restart=on-failure
RestartSec=10s
LimitNOFILE=40000

[Install]
WantedBy=multi-user.target
EOF

systemctl enable etcd
systemctl daemon-reload
systemctl start etcd