<div align="center">
<!-- INFO: The empty line is required for center to work.-->

[![Nix](https://img.shields.io/badge/Nix_devShell-%235277C3?style=for-the-badge&logo=NixOS&logoColor=white)](https://nixos.wiki/wiki/Flakes)
[![Helm](https://img.shields.io/badge/Helm-0F1689?style=for-the-badge&logo=Helm&logoColor=white)](https://helm.sh/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink%201.18.1-E6526F?style=for-the-badge&logo=Apache%20Flink&logoColor=white)](https://flink.apache.org/)
[![Chameleon Cloud](https://img.shields.io/badge/Chameleon%20Cloud-042F55D?style=for-the-badge&logoColor=white)](https://www.chameleoncloud.org/)

</div>

# Description
This Projects contains all parts of the UpStream scaling framework. It makes use of domain specific metrics from the Flink application cluster to make a scaling decision using
[MPC](https://en.wikipedia.org/wiki/Model_predictive_control).
# Setup

> [!TIP]
> This repo makes use of [git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules).
> To clone with all included submodules at once you can run:
> ``` bash
> git clone --recurse-submodules git@github.com:DPS-Stream-Processing-Applications/UpStream.git
> ```

This project contains a `flake.nix` file to manage all the dependencies that would usually need to be installed through a regular package manager
with the Nix package manager instead. The Nix Shell will provide an installation of Flink as well as the OpenJDK needed for the Gradle wrapper to work.

## Installing Nix
To install Nix follow the [official instructions](https://nixos.org/download) for your platform.
Following this, you need to enable `flakes` and `nix-command` for the Nix package manager that you just installed.
The "Other Distros, without Home-Manager" section of the [Flake Wiki](https://nixos.wiki/wiki/Flakes) will explain how to do this.
> [!TIP]
> If the `~/.config/nix` folder and `nix.conf` file do not already exist after installing, you need to create them manually.

## Nix Develop
After successfully installing Nix and enabling Flakes, you will be able to use the `nix develop` command in the root of the project to enter a
development shell managed by Nix. To exit the dev shell, use the `exit` command or hit `Ctrl+d`.

## Direnv
> [!NOTE]
> This step is entirely optional but may improve your development experience.

Using [Direnv](https://direnv.net/) will allow you to automatically launch the Nix `devShell` whenever you change into the project directory.
Direnv can also be detected by your IDE if a plugin exists.
You might also want to install [nix-direnv](https://github.com/nix-community/nix-direnv) to improve the Direnv experience with Nix.

## Kubernetes
For all the applications in this repository external resources are required.
These resources are all managed within a Kubernetes cluster.

### Local K8S Cluster
> [!IMPORTANT]
> Make sure you have [docker](https://www.docker.com/) installed on your system before reading further.

The nix dev shell provides `k3d` as means to spin up a local Kubernetes cluster.
Use the `k3d-cluster-config.yaml` file of this project to set up a preconfigured `test-cluster`.
See the following [guide on how to use a config file](https://k3d.io/v5.0.0/usage/configfile/#usage).
Follow the [quick start guide](https://k3d.io/v5.6.3/#quick-start) to set up an empty cluster.

### Remote Cluster with Chameleon Cloud
This project uses the Chameleon test bed more specificaly `CHI@TACC` to test and evaluate the application.
The experimental setup requires 1 Jump Host as well as 3 cluster nodes.
Required resources: 1 floating IP, One instance of type skylake, as well as three instances of a cascadelake node.
To provision instances on Chameleon Cloud you first have to [create a lease](https://chameleoncloud.readthedocs.io/en/latest/technical/reservations/gui_reservations.html).
After that you can [launch the instances](https://chameleoncloud.readthedocs.io/en/latest/technical/baremetal/launching_gui.html#launch-an-instance).
Make sure you use the official Chameleon Cloud version of the `Ubuntu 26.04` image for consistency.

> [!TIP]
> regarding `ssh` access to the host, it might be easiest to upload your existing global ssh key,
> such that all following `ssh` commands work without modification.

#### SSH
To make connecting to the test bed easier, this project provides an ssh config located at`.ssh/config`.
Make sure you edit the IP addresses of the nodes once they are running in Chameleon.

#### Ansible
This project uses `Ansible` to set up the Kubernetes cluster on the remote nodes.
Follow the setup instructions in the [ansible directory](ansible/README.md).

#### Forwarding kubectl
`kubectl` uses port 6443 to communicate with a cluster.
The `k3s-server` node is only accessible via the jump host. This means if we want to use `kubectl` locally we need to modify the `.kube/config`.
The `fix_kubeconfig` utility does this for you. The next step is to forward traffic from `localhost` to `k3s-server`.
This project provides the `forward_kubectl` utility to achieve port forwarding from localhost:6443 to the `k3s-server` node.

> [!WARNING]
> This command will only work once the [Ansible setup](#ansible) has been completed.

```bash
fix_kubeconfig && forward_kubectl
```

You can verify connectivity to the Kubernetes cluster by running 

```bash
kubectl get nodes
```

### Helm Deployment
All the applications of this repository depend on external resources like an Apache `Kafka` cluster as well as a `mongoDB` database.
All dependencies for the applications are managed and deployed via a custom `helm chart` in the `helm/` directory.
Refer to the [README](./helm/README.md) for an installation walkthrough.

## Monitoring
The project is using Prometheus and Grafana to track the metrics of the evaluation runs
To forward the Monitoring UI's as well as the Flink UI run 
```bash
forward_monitoring
```

### Grafana Credentials
You will need to query the login credentials for Grafana using the following commands:

```bash
kubectl get secret monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 --decode && echo
kubectl get secret monitoring-grafana -o jsonpath="{.data.admin-user}" | base64 --decode && echo
```

There are 2 dashboards as `.json` files stored in the [helm folder](./helm)
