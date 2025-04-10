#!/usr/bin/env bash
minikube delete
minikube start


minikube addons enable metrics-server
minikube addons enable ingress

eval $(minikube docker-env)

docker build -t order:latest ./order
docker build -t stock:latest ./stock
docker build -t payment:latest ./payment

# redis stuff
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install -f helm-config/redis-order-db-helm-values.yaml db-order bitnami/redis
helm install -f helm-config/redis-stock-db-helm-values.yaml db-stock bitnami/redis
helm install -f helm-config/redis-payment-db-helm-values.yaml db-payment bitnami/redis
helm install -f helm-config/redis-events-helm-values.yaml db-event bitnami/redis

kubectl apply -f ./k8s

echo "Minikube IP address:"
minikube ip
echo "PLEASE PASTE THIS IP: $(minikube ip) IN THE WDM-Benchmark tool's urls.json file!!!!!!"
echo "PLEASE PASTE THIS IP: $(minikube ip) IN THE WDM-Benchmark tool's urls.json file!!!!!!"
echo "PLEASE PASTE THIS IP: $(minikube ip) IN THE WDM-Benchmark tool's urls.json file!!!!!!"
echo "PLEASE PASTE THIS IP: $(minikube ip) IN THE WDM-Benchmark tool's urls.json file!!!!!!"
echo "PLEASE PASTE THIS IP: $(minikube ip) IN THE WDM-Benchmark tool's urls.json file!!!!!!"

echo "Now Run minikube dashboard to see the deployments and dbs, and kill whichever container/pod you desire."
echo "Please wait 1-2 minutes till everything is deployed"
echi "Also, when deleting containers, it takes approximately 1 minute for the replica to take over. Be patient, the system recovers :)"