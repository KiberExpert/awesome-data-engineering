🚀 GCP Project Setup via Python Script
This project demonstrates how to programmatically create and configure a Google Cloud Platform (GCP) project using a Python script. It is designed to automate the process of provisioning GCP resources, making it easier for data engineering and cloud teams to deploy consistent environments.

⚙️ Features
Create a GCP project via Python

Set billing account and organization

Enable necessary APIs

Create default folder and IAM bindings

Add service accounts and roles

🧰 Technologies Used
Python 3.9+

Google Cloud SDK

google-api-python-client

google-auth

PyYAML (for config files)

📝 Prerequisites
Active GCP account

gcloud CLI installed and authenticated

Billing account ID

Organization ID

Required APIs enabled:

Cloud Resource Manager

Billing API

IAM API

Service Usage API

🚦 Installation

# Clone the repo
git clone https://github.com/your-repo/gcp-project-creator.git
cd gcp-project-creator

# Create virtual environment
python -m venv venv
source venv/bin/activate  # For Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

🚀 Usage

python create_project.py --project_id your-project-id --billing_account YOUR_BILLING_ID --org_id YOUR_ORG_ID
Or use the YAML config:


python create_project.py --config config/project_config.yaml
📌 Notes
Make sure your authenticated user/service account has the necessary permissions to create GCP projects and assign roles.

Review the IAM bindings and project policies in your config file before running in production environments.

📤 Output
A new GCP project will be created

APIs will be enabled

IAM roles assigned to service accounts or users