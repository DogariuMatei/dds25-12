You need `minikube` and `kubectl` installed locally

```
minikube start --driver=docker
```
Run this: `deploy-charts-minicube.sh` (its in the root directory of the project)
```
eval $(minikube docker-env)
```
```
minikube addons enable ingress
```
```
docker build -t order:latest ./order
docker build -t stock:latest ./stock
docker build -t payment:latest ./payment
```
```
kubectl apply -f ./k8s
```
Leave this running:
```
kubectl port-forward -n ingress-nginx service/ingress-nginx-controller 8080:80
```

Leave this running too in separate terminal:
```
minikube dashboard
```
