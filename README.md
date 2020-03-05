

## Deploy on Openshift 

### Step1 : Initial setup of secrets

To deploy osa-data-collector we have to deploy asw as well as big query related secretes into openshift namespace. 

AWS secrets we are using to push osa-data-collector output to S3 object. Here is the sample definition of the secret:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: aws
type: Opaque
data:
  aws_access_key_id: <Your base 64 encoded access key here>
  aws_secret_access_key: <Your base 64 encoded secret key here>
```
To base64 encode a piece of text, use the command:
```bash
echo -n "<Your text here>" | base64
```
**N.B.** on a Mac install and use the command `gbase64` instead of the default `base64`.

Save the full yaml definition to a file, say `aws-secret.yaml` and use this command
to apply the secret to your namespace:

```bash
oc apply -f aws-secret.yaml
```

Similarly, base64 encode the complete contents of the bigquery service account `key.json` file as is and create
a new secret definition, say `google-secret.yaml`.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: google-services-secret
type: Opaque
data:
  google-services.json: |-
    <Full multi-line secret content goes here.>
```
If everything went according to plan till this step, you now have the following two secrets on the cluster:
```bash
$ oc get secrets
NAME                       TYPE                                  DATA   AGE
aws                        Opaque                                2      22h
google-services-secret     Opaque                                1      22h
```

You can now proceed with the deployment.

### Step2 : Build Container image (Optional)

You can perform this changes if you want to test your local changes on openshift cluster. Run below command to build docker image. 
```
docker build --no-cache -t <image_name:tag> ./
```

### Step3 : Push container image to registry (Optional)

If you are performing step2 then you can continue with this step else skip it. Push created container to some registry. (e.g. quay.io)

```
docker push <registry>/<image_name:tag>
```

### Step4 : Run the job on openshift cluster. 

There are two ways, we can run data-collector job in openshift cluster

#### One Time Job
* We can deploy `one time` job using `template-job.yaml` file. Use `oc` to deploy it on OpenShift
```
oc process -f openshift/template-job.yaml | oc create -f -
```

#### Cron Job
* We can deploy `cron` job using `template-cronjob.yaml` file. Use `oc` to deploy it on OpenShift
```
oc process -f openshift/template-cronjob.yaml | oc create -f -
```
**Note** : If you want to change any parameter defined in template file you can pass it along with command. 
In Below example we are passing docker registry and image name. Similar way you can pass other required parameter. 
```
oc process -f openshift/template-job.yaml DOCKER_REGISTRY=<registry> -p DOCKER_IMAGE=<image_name> | oc create -f -
```
## Run locally for dev testing 

### Step1 : Setup few environment variables

Make Sure you have following AWS environment variables present into your system, which are require to connect to s3 bucket and upload data-collector output. 
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY 
- AWS_S3_BUCKET_NAME 

Also store big query related credential into one file and save absolute path into "BIGQUERY_CREDENTIALS_FILEPATH" environment variable. This will be used to query data from big query. 


### Step2 : Install dependency

Install python dependacies using following command. 
```bash
pip install -r requirements.txt 
```
### Step3 :  Run Main logic

You can run data-collector program using following command. 
```bash
python run_data_collector.py -e openshift knative kubevirt -d 1
```
Command contains two parameters. 
* -e : The eco-systems to monitor. Options available for now  are [openshift knative kubevirt]
* -d : The number of days data to retrieve from GitHub including yesterday

