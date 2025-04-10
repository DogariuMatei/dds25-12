How to run:
----------
You need `minikube` and `kubectl` installed locally
---------
We wrote a bash script that runs everything by itself `RUN-ME-IF-LINUX.sh` or `RUN-ME-IF-WINDOWS-WSL.sh`, make sure to run it, wait, and PAY ATTENTION TO THE LAST LINE IN THE OUTPUT. You have to paste the right IP into the WDM benchmark `urls.json` file.
After it finishes and you've pasted what you needed to, please run `minikube dashboard` to see pods status, kill them, restart them, etc.

-------
Manual run:
```
minikube start --driver=docker
```
```
minikube addons enable metrics-server
minikube addons enable ingress
```
Run this: `deploy-charts-minikube.sh` (its in the root directory of the project)
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
