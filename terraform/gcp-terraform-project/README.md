````md
## 🚀 Deployment Steps

1. **Clone the Repository**
   ```bash
   git clone <your-repo-url>
   cd <your-repo-folder>
````

2. **Initialize Terraform**

   ```bash
   terraform init
   ```

3. **Review the Execution Plan**

   ```bash
   terraform plan
   ```

4. **Apply the Configuration**

   ```bash
   terraform apply
   ```

5. **Verify the Deployment**
   Open your **GCP Console** and confirm the following resources were created:

   * ☁ Google Storage Bucket
   * ⏰ Cloud Scheduler Job
   * 🛠 Cloud Run Service
   * 📦 Container Registry
   * 🔑 IAM Role Bindings

```

```
