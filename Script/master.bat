for /d %%i in (*.etcd) do (  
    rmdir /s /q "%%i"  
)  

etcd -name etcd-%1 -listen-peer-urls http://0.0.0.0:2380 -listen-client-urls http://0.0.0.0:2379 -advertise-client-urls http://%1:2379 -initial-advertise-peer-urls http://%1:2380 -initial-cluster-token etcd-cluster -initial-cluster "etcd-%1=http://%1:2380" -initial-cluster-state new