#!/usr/bin/env bash

#minikube start
#minikube addons enable ingress
./deploy-charts-minikube.sh

eval $(minikube docker-env)
docker build -t order:latest ./order
docker build -t stock:latest ./stock
docker build -t user:latest ./payment
kubectl apply -f ./k8s

#kubectl port-forward -n ingress-nginx service/ingress-nginx-controller 8000:80
#minikube dashboard