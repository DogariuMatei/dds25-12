How to run:
----------
You need `minikube` and `kubectl` installed locally
---------
We wrote a bash script that runs everything by itself `deploy-minikube-custom.sh`, make sure to run it, wait, and PAY ATTENTION TO THE LAST LINE IN THE OUTPUT. You have to paste minikubes ip into the WDM benchmark `urls.json` file.
After it finishes and you've pasted what you needed to, please run `minikube dashboard`

-------
Manual run:
```
minikube start --driver=docker
```
```
minikube addons enable metrics-server
minikube addons enable ingress
```
Run this: `deploy-charts-minicube.sh` (its in the root directory of the project)
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
Leave this running:
```
kubectl port-forward -n ingress-nginx service/ingress-nginx-controller 8080:80
```

Leave this running too in separate terminal:
```
minikube dashboard
```
