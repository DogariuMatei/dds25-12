# DDS2025: Group12 Alternate Version

This version uses the same system architecture as present on main branch.
The main difference is the deployment to ensure consistency and availability.

This version uses Redis Sentinels to ensure consistency in the DBs, each having a single replica. 
We also employ Watchdogs to monitor every service, starting up services that have failed. 
It acts as a container health monitor.
### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment:

Put system IP & current project directory in the `.env` file.
Then use these commands:
- `docker-compose build`
- `docker-compose --env-file .env up`

System should now be fully functional.

### Issues:

This version is unable to scale properly. 
Also, please note:
- Failing the `gateway` will render the system useless because watchdogs cannot revive the gateway due to mounting issues. 
- Only 1 service should be failed at a time (to ensure it is assigned the same IP as before upon restart by watchdogs). This is due to the weird DNS caching issues that were unable to be fixed with the gateway.
- DBs can be failed at will. Just ensure the slaves are not failed either, else data will be lost. 

System should now be fully functional.