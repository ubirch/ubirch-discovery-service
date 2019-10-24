
install:
```
helm.sh dev install --name discovery-service-kafka discovery-service-kafka --values discovery-service-kafka/values.dev.yaml --namespace ubirch-dev
```

remove:
```
helm.sh dev delete discovery-service-kafka --purge
```

check status:
```
helm.sh dev list
k8sc.sh dev get pods -n ubirch-dev
```
