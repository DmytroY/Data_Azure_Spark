# Spark ETL on AKS
## Prerequisites
Installed latest:   
- Azure CLI, 
- terraform, 
- docker, 
- spark installed to /opt/spark/
- setuptools

Login to Azure:
```
az login
```

## Terraform   
Create Azure Resource Group and Storage Account, get Storage Account key:
```
az group create --name tf-state-rg \
  --location westeurope

az storage account create --name sa4dytfstate \
  --location westeurope \
  --resource-group tf-state-rg

az storage account keys list --account-name sa4dytfstate
```
Use key from abow, create a container so Terraform can store the state management file:
```
az storage container create --account-name sa4dytfstate \
  --name tfstate \
  --public-access off \
  --account-key <account-key>
```
Initialize Terraform working directory, create an execution plan and save the plan to the file, run action plan
```
cd terraform
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```
When terraform ask for prefix use "yakovd1", either data in config.json should be changed respectively.

To use cubectl with created AKS cluster:
```
az aks get-credentials --name aks-yakovd1-westeurope --resource-group rg-yakovd1-westeurope
```
verify the connection: 
```
kube ctl gety nodes
kubectl cluster-info
```

Do not forget to destroy infrastructure after completing spark job.

## Data
Put your data to the Azure storage account blob container. Now it can be accessible from pyspark console. Just start pyspark this way:
```
pyspark \
  --conf spark.hadoop.fs.azure.account.key.<acc>.dfs.core.windows.net=<key> \
  --packages org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.3
```
where:   
**acc** - storage account name,   
**key** - storage account key.  

You can access the data by such path:  
```
data_path = f"abfs://{container}>@{account}.dfs.core.windows.net"
```

## Build
python3 setup.py build bdist_egg

## Docker
Use Azure container registry created by terraform, or 
create new Azure container registry, login to it:
```
az group create --name RG-4ContainerRegistry --location eastus
az acr create --resource-group RG-4ContainerRegistry \
  --name dycr1 --sku Basic

az acr login --name dycr1
```

build docker image (option -p is for generating pyspark image):
```
docker-image-tool.sh \
  -r dycr1.azurecr.io \
  -t v1 \
  -p docker/Dockerfile build
```

Push docker image to Azure container
```
docker-image-tool.sh -r dycr1.azurecr.io -t v1 push
```

## Run Spark job on AKS
```
spark-submit \
  --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
  --deploy-mode cluster \
  --name sparkbasics \
  --conf spark.kubernetes.container.image=<spark-image> \
  --conf spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net=<key> \
  --packages org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.3 \
  src/main.py
```
