# Deploy to Vercel

## What Gets Deployed

Only the **UI Dashboard** (`/ui` folder) will be deployed to Vercel as a static site with simulated data.

**Note:** The backend services (PostgreSQL, MinIO, Airflow, etc.) require Docker and cannot run on Vercel. They must be run locally or on a cloud VM.

## Deployment Steps

### Option 1: Deploy via Vercel Dashboard (Easiest)

1. Go to https://vercel.com/princedubey45s-projects
2. Click **"Add New Project"**
3. Click **"Import Git Repository"**
4. Select: `princedubey45/Real-Time-Data-Lakehouse-with-Predictive-Analytics`
5. Configure:
   - **Framework Preset:** Other
   - **Root Directory:** `./` (leave as default)
   - **Build Command:** Leave empty
   - **Output Directory:** `ui`
6. Click **"Deploy"**

### Option 2: Deploy via Vercel CLI

```bash
# Install Vercel CLI
npm install -g vercel

# Login to Vercel
vercel login

# Deploy
vercel --prod
```

## What Will Work on Vercel

✅ UI Dashboard with simulated data  
✅ All charts and visualizations  
✅ Navigation between pages  
✅ Responsive design  

## What Won't Work on Vercel

❌ Real data from PostgreSQL  
❌ Real data from MinIO  
❌ Airflow integration  
❌ Running Python ETL scripts  
❌ Docker services  

## For Full Functionality

To run the complete data platform with all services:

1. Clone the repo locally
2. Run: `docker-compose up -d`
3. Run: `cd ui && python3 -m http.server 8000`
4. Access: http://localhost:8000

## Alternative Cloud Deployment Options

For the full stack (backend + frontend):

- **AWS EC2** - Run Docker Compose on a VM
- **Google Cloud Compute Engine** - Run Docker Compose on a VM
- **DigitalOcean Droplet** - Run Docker Compose on a VM
- **Railway.app** - Supports Docker Compose
- **Render.com** - Supports Docker Compose

## Live Demo

Once deployed to Vercel, your UI will be available at:
`https://your-project-name.vercel.app`
