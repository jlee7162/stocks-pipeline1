
# Stocks Severless Pipeline

## Architecture
EventBridge → api_fetch Lambda → DynamoDB
API Gateway → retrieval Lambda → DynamoDB
GitHub Actions → Quarto render → S3

## Prerequisites
- AWS CLI configured
- AWS CDK installed
- Python 3.11+
- Node.js

## Deploy
1. Clone repo
```
git clone https://github.com/jlee7162/stocks-pipeline1.git
cd stocks-pipeline1
```

2. Create virtual environment
```
python3 -m venv .venv
source .venv/bin/activate 
```

3. Install dependencies
```
pip install -r requirements.txt
```

4. Install Quarto
```
wget https://github.com/quarto-dev/quarto-cli/releases/download/v1.6.39/quarto-1.6.39-linux-amd64.deb
sudo dpkg -i quarto-1.6.39-linux-amd64.deb
```

5. Configure AWS
```
aws configure
```

6. Create secret
```
aws secretsmanager create-secret \
  --name "stocks/massive-api-key" \
  --secret-string '{"MASSIVE_API_KEY": "your_key"}'
```

7. Deploy
```
cdk bootstrap #only for first time
cdk deploy
```

8 Add GitHub secrets
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY
   - S3_BUCKET_NAME

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!
