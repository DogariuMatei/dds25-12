# How to run:
----------
## You need `minikube` and `kubectl` installed locally
---------
We wrote a bash script that runs everything by itself `RUN-ME-IF-LINUX.sh` or `RUN-ME-IF-WINDOWS-WSL.sh`. We hope you're using some sort of Linux bc you will have less of a headache.
Make sure to run it, wait, and PAY ATTENTION TO THE LAST LINE IN THE OUTPUT. You have to paste the right IP into the WDM benchmark `urls.json` file.  

After it finishes and you've pasted what you needed to, please run `minikube dashboard` to see pods status, kill them, restart them, etc.

-------
## Manual run (if everything else fails):
```
minikube start --driver=docker
```
```
minikube addons enable metrics-server
minikube addons enable ingress
```
```
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install -f helm-config/redis-order-db-helm-values.yaml db-order bitnami/redis
helm install -f helm-config/redis-stock-db-helm-values.yaml db-stock bitnami/redis
helm install -f helm-config/redis-payment-db-helm-values.yaml db-payment bitnami/redis
helm install -f helm-config/redis-events-helm-values.yaml db-event bitnami/redis
```

```
eval $(minikube docker-env)
```
```
docker build -t order:latest ./order
docker build -t stock:latest ./stock
docker build -t payment:latest ./payment
```
```
kubectl apply -f ./k8s
```
Leave this running - this will 100% work on any os you might be on. Performance will take a hit if you use this:
```
kubectl port-forward -n ingress-nginx service/ingress-nginx-controller 8080:80
```

Leave this running too in separate terminal:
```
minikube dashboard
```
