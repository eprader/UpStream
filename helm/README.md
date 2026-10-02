# Setup
Follow all these commands in exactly this order to guarantee a smooth setup.

```bash
helm install \
cert-manager oci://quay.io/jetstack/charts/cert-manager \
--version v1.15.0 \
--namespace cert-manager \
--create-namespace \
--set crds.enabled=true
```

```bash
helm install mongodb ./mongodb
helm install kafka ./kafka
helm install monitoring ./monitoring
```
These three charts contain the required infrastructure for the RIoT applications and monitoring.

Before you are able to run the Flink application cluster you need to upload the Job jar you are planning to deploy.
The current implementation expects the job to be available on local disk.
To upload the `.jar` build artifact to the cluster you can use the `upload_job_jar` utility.

```bash
upload_job_jar ../beam-applications-java/etl/build/FlinkJob.jar
upload_job_jar ../beam-applications-java/stats/build/FlinkJob.jar
```

```bash
helm install flink ./flink
# or
helm install flink ./flink -f ./flink/upstream_values.yaml
helm install flink ./flink -f ./flink/with_hpa_values.yaml
```
> [!WARNING]
> Check `kubectl get pods` after installing the Flink chart.
> It might not have launched the Jobmanager and Taskmanager
> pods (starting with ´flink-application-cluster...`).
> Uninstall the chart and reinstall it.
> After a second install the pods should be there.

Sometimes reinstalling flink will fail because some artifacts of the previous install are not removed via `helm uninstall`
The following commands came up during implementation and testing to remove left behind artifacts.

```bash
kubectl delete flinkdeployments --all

kubectl patch flinkdeployment flink-application-cluster -n default --type=json -p '[{"op": "remove", "path": "/metadata/finalizers"}]'

kubectl delete serviceaccount flink -n default

kubectl delete role flink -n default

kubectl delete rolebinding flink-role-binding
```
