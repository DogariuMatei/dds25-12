#!/usr/bin/env bash

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install -f helm-config/redis-order-db-helm-values.yaml order-db bitnami/redis
helm install -f helm-config/redis-stock-db-helm-values.yaml stock-db bitnami/redis
helm install -f helm-config/redis-payment-db-helm-values.yaml payment-db bitnami/redis
helm install -f helm-config/redis-events-helm-values.yaml event-db bitnami/redis
