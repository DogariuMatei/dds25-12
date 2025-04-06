#!/usr/bin/env bash
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

helm install -f helm-config/redis-order-db-helm-values.yaml order-db bitnami/redis
helm install -f helm-config/redis-stock-db-helm-values.yaml stock-db bitnami/redis
helm install -f helm-config/redis-payment-db-helm-values.yaml payment-db bitnami/redis
helm install -f helm-config/redis-events-helm-values.yaml event-db bitnami/redis

kubectl apply -f ./k8s

echo "Minikube IP address:"
minikube ip
echo "PLEASE PASTE THIS IP: $(minikube ip) IN THE WDM-Benchmark tool's urls.json file!!!!!!"