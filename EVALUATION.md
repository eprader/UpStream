To evaluate the scaler we need to set up the Kafka event generator implemented in `./beam-applications-java/kafkaProducer/` on the ssh jump host of the cluster.

This is required because kafka will only serve the ipaddresses of the pods which are not available from outside the Chameleon Cloud network.

## Dataset Prep
Make sure you created you required dataset using the [senMLScenarioBuilder](./senMLScenarioBuilder/)

## Event generator setup
To upload the eventgenerator to the bastion and generate events for the pipeline starting there:

```bash
set_up_bastion
```
ssh into the jump host using `ssh jump`.
## Forwarding

In order for portforwaring of kubectl to work make sure you manually ssh to all the instances before proceeding

```bash
ssh k3s-server
ssh k3s-agent-1
ssh k3s-agent-2
```
Now you can run the forwarding utilities

```bash
forward_kubectl
forward_kafka
```

### Stop forwarding:
It might be teh case that the forwarding precess gets stuck in the background you can run the following command to kill them:

```bash
lsof -ti :6443 | xargs kill
lsof -ti :9093 | xargs kill
```


```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_constant_rate_scenario.csv $(nproc) "senml-source"
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_constant_rate_scenario.csv $(nproc) "senml-cleaned"
```

## Downloading benchmark data
