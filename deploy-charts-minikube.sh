#!/usr/bin/env bash

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install -f helm-config/redis-helm-values.yaml redis bitnami/redis
helm install my-release -f helm-config/kafka-helm-values.yaml oci://registry-1.docker.io/bitnamicharts/kafka