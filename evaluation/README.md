To evaluate the scaler we need to set up the Kafka event generator implemented in `./beam-applications-java/kafkaProducer/` on the ssh jump host of the cluster.

This is required because kafka will only serve the ipaddresses of the pods which are not available from outside the Chameleon Cloud network.

## Dataset Prep
Make sure you created you required dataset using the [senMLScenarioBuilder](./senMLScenarioBuilder/)

## Event generator setup
To upload the eventgenerator to the bastion and generate events for the pipeline starting there:

```bash
set_up_bastion
ssh jump
cd event_generator
nix develop
```
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
It might be the case that the forwarding precess gets stuck in the background you can run the following command to kill them:

```bash
lsof -ti :6443 | xargs kill
lsof -ti :9093 | xargs kill
```



## Downloading benchmark data

```bash
uv run python main.py 2026-08-14T16:56:31 2026-08-14T17:16:51 ./values.parquet
```



# Baseline without Scaling

We compare a baseline  of ETL since it is the most linear DAG, with the maximum size of taskmanager (small) with 64 replicas
and (taskmanager-large) with 8 replicas to show the overhead of networking / the gains of joining taskmanagers to bigger instances.

For 64 replicas we can see that running the event generator with 10000 events per second bottlenecks (backpressures) at only ~ 900 events per second.

Start of run at 2026-08-14 16:57:30

large with 8 replicas 20000 events per second replicas start at 

Start of run at 2026-08-14 21:44:01Bottlenecking at ~ 12500 events per second

Establish 10000 Events per second as the upper limit for all future experiments to allow for some headroom.


# Static Load

```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_constant_rate_scenario.csv $(nproc) "senml-source"
```
## ETL

UPSTREAM setup:
10000 events/s senml-cleaned
2026-08-15 20:15:56 until 2026-08-15 20:41:00

## STATS
```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_constant_rate_scenario.csv $(nproc) "senml-cleaned"
```

## PRED

```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_constant_rate_scenario.csv $(nproc) "senml-cleaned"
```

# Normalized Realistic TAXI
## ETL 
```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_15min_normalized.csv $(nproc) "senml-source"
```

## STATS
```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_15min_normalized.csv $(nproc) "senml-cleaned"
```

## PRED
```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_15min_normalized.csv $(nproc) "senml-cleaned"
```
# Exponential Load

## ETL
UPSTREAM:
```bash
java -jar $PROJECT_ROOT/KafkaProducer.jar $PROJECT_ROOT/data/riot_events_TAXI_exponential_scenario.csv $(nproc) "senml-source"
```

# Plotting
```bash
python compare_groups.py ./exponential_plateau "ETL_HPA" "ETL_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1
python compare_groups.py ./static_10000 "ETL_HPA" "ETL_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1
python compare_groups.py ./TAXI_normalized "ETL_HPA" "ETL_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1

python compare_groups.py ./static_10000 "STATS_HPA" "STATS_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1
python compare_groups.py ./TAXI_normalized "STATS_HPA" "STATS_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1

python compare_groups.py ./static_10000 "PRED_HPA" "PRED_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1
python compare_groups.py ./TAXI_normalized "PRED_HPA" "PRED_UpStream" --label-a HPA --label-b UpStream --backpressure-threshold 0.1

cd comparison_plots
python plot_upstream_vs_hpa.py
cd ..
```

