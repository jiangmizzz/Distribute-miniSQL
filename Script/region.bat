for /d %%i in (*.etcd) do (  
    rmdir /s /q "%%i"  
)

for /f tokens^=2^ delims^=^" %%i in ('etcdctl --endpoints=http://%2:2379 member add etcd-%1 --peer-urls=http://%1:2380 ^| find "ETCD_INITIAL_CLUSTER="') do set cluster=%%i

etcd -name etcd-%1 -listen-peer-urls http://0.0.0.0:2380 -listen-client-urls http://0.0.0.0:2379 -advertise-client-urls http://%1:2379 -initial-advertise-peer-urls http://%1:2380 -initial-cluster %cluster% -initial-cluster-state existing