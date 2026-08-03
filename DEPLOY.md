# Déploiement — Extraction Pipeline GCP

Projet GCP : `dynamic-camp-465312-b6`  
Région : `europe-west1`  
Service Account : `extraction-runner@dynamic-camp-465312-b6.iam.gserviceaccount.com`

---

## 0. Se connecter au bon projet GCP

```bash
gcloud auth login
gcloud auth application-default login
gcloud auth application-default set-quota-project dynamic-camp-465312-b6
```

---

## 1. Mettre à jour l'image après modification du code

```bash
# Rebuild + push
gcloud builds submit --tag europe-west1-docker.pkg.dev/dynamic-camp-465312-b6/envergure/pipeline:latest

# Mettre à jour le job Cloud Run avec la nouvelle image
gcloud run jobs update extraction-quotidienne \
  --image=europe-west1-docker.pkg.dev/dynamic-camp-465312-b6/envergure/pipeline:latest \
  --region=europe-west1
```

---

## 2. Exécuter le job manuellement

```bash
gcloud run jobs execute extraction-quotidienne --region=europe-west1
```

Suivre les logs en temps réel :
```bash
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=extraction-quotidienne" \
  --project=dynamic-camp-465312-b6 \
  --limit=50 \
  --format="value(timestamp, textPayload)" \
  --freshness=1h
```

---

## 3. IP fixe sortante (à faire avant mise en prod)

### 3a. Réserver une IP statique
```bash
gcloud compute addresses create extraction-nat-ip \
  --region=europe-west1 \
  --project=dynamic-camp-465312-b6
```

Récupérer l'IP (à transmettre à SC-Form pour whitelist) :
```bash
gcloud compute addresses describe extraction-nat-ip --region=europe-west1 --format="get(address)"
```

### 3b. Créer un réseau VPC dédié
```bash
gcloud compute networks create envergure-vpc \
  --subnet-mode=custom \
  --project=dynamic-camp-465312-b6

gcloud compute networks subnets create envergure-subnet \
  --network=envergure-vpc \
  --region=europe-west1 \
  --range=10.8.0.0/28 \
  --project=dynamic-camp-465312-b6
```

### 3c. Créer le VPC Connector (pour Cloud Run → VPC)
```bash
gcloud compute networks vpc-access connectors create envergure-connector \
  --network=envergure-vpc \
  --region=europe-west1 \
  --range=10.8.0.0/28 \
  --project=dynamic-camp-465312-b6
```

### 3d. Cloud Router + Cloud NAT avec l'IP statique
```bash
gcloud compute routers create envergure-router \
  --network=envergure-vpc \
  --region=europe-west1 \
  --project=dynamic-camp-465312-b6

gcloud compute routers nats create envergure-nat \
  --router=envergure-router \
  --region=europe-west1 \
  --nat-external-ip-pool=extraction-nat-ip \
  --nat-custom-subnet-ip-ranges=envergure-subnet \
  --project=dynamic-camp-465312-b6
```

### 3e. Attacher le VPC Connector au job Cloud Run
```bash
gcloud run jobs update extraction-quotidienne \
  --region=europe-west1 \
  --vpc-connector=envergure-connector \
  --vpc-egress=all-traffic
```

---

## 4. Automatiser avec Cloud Scheduler

```bash
# Créer le déclencheur (tous les jours à 6h00 heure Paris)
gcloud scheduler jobs create http extraction-quotidienne-cron \
  --location=europe-west1 \
  --schedule="0 6 * * *" \
  --time-zone="Europe/Paris" \
  --uri="https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/dynamic-camp-465312-b6/jobs/extraction-quotidienne:run" \
  --http-method=POST \
  --oauth-service-account-email=extraction-runner@dynamic-camp-465312-b6.iam.gserviceaccount.com

# Donner au SA le droit de déclencher des jobs Cloud Run
gcloud projects add-iam-policy-binding dynamic-camp-465312-b6 \
  --member="serviceAccount:extraction-runner@dynamic-camp-465312-b6.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

---

## 5. Variables d'environnement sur Cloud Run (si besoin)

```bash
gcloud run jobs update extraction-quotidienne \
  --region=europe-west1 \
  --set-env-vars="SOFIA_PASSWORD=<val>,MYSQL_PASSWORD=<val>"
```

Ou via Secret Manager (recommandé) :
```bash
gcloud run jobs update extraction-quotidienne \
  --region=europe-west1 \
  --set-secrets="SOFIA_PASSWORD=sofia-password:latest,MYSQL_PASSWORD=mysql-password:latest"
```

---

## 6. Secrets dans Secret Manager

```bash
# Créer un secret
echo -n "<valeur>" | gcloud secrets create <nom-secret> --data-file=-

# Mettre à jour un secret existant
echo -n "<nouvelle-valeur>" | gcloud secrets versions add <nom-secret> --data-file=-

# Donner accès au Service Account
gcloud secrets add-iam-policy-binding <nom-secret> \
  --member="serviceAccount:extraction-runner@dynamic-camp-465312-b6.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```
