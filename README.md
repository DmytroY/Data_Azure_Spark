### Prerequisites
Installed latest:   
Azure CLI, terraform

## Terraform   
login to azure:
```
az login
```
Create Azure Resource Group and Storage Account, get Storage Account key:
```
az group create --name tf-state-rg \
  --location westeurope

az storage account create --name sa4tdyfstate \
  --location westeurope \
  --resource-group tf-state-rg

az storage account keys list --account-name sa4tdyfstate
```
Use key from abow, create a container so Terraform can store the state management file:
```
az storage container create --account-name sa4tdyfstate \
  --name tfstate \
  --public-access off \
  --account-key <account-key>
```
Initialize Terraform working directory, create an execution plan and save the plan to the file, run action plan
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```
When terraform ask for prefix use "yakovd", either data in config.json should be changed respectively.

Do not forget to destroy infrastructure after completing spark job!

## Build
python3 setup.py build bdist_egg

## Docker
2 environment variables shoul be assigned for run: OPENCAGE_API_KEY and AZ_STORAGE_ACCES_KEY

to build docker image: docker build -t sparkbasics . -f docker/Dockerfile
Environment variable can be passed to docker image at run: docker run -it -e MYVAR="This is myvar" {dockerimagename}


================= ORIGINAL README =============================
* Setup needed requirements into your env `pip install -r requirements.txt`
* Add your code in `src/main/`
* Test your code with `src/tests/`
* Package your artifacts
* Modify dockerfile if needed
* Build and push docker image
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch Spark app in cluster mode on AKS
```
spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name sparkbasics \
    --conf spark.kubernetes.container.image=<spark-image> \
    ...
```
