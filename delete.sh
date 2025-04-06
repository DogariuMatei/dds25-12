#!/usr/bin/env bash

kubectl delete service order-service
kubectl delete service stock-service
kubectl delete service user-service
kubectl delete ingress ingress-service
kubectl delete deployments order-deployment
kubectl delete deployments stock-deployment
kubectl delete deployments user-deployment
kubectl delete replicaset --all

kubectl delete service redis-master
kubectl delete service redis-replicas
kubectl delete service redis-headless
kubectl delete service my-release-kafka
kubectl delete service my-release-kafka-controller-headless
kubectl delete statefulset --all
kubectl delete deployments --all
kubectl delete pod --all
kubectl delete pvc --all
kubectl delete pv --all
kubectl delete ingressclass --all
helm ls --all --short | xargs -L1 helm delete