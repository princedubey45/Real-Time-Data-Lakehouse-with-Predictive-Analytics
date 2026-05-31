# Deploy to Render.com

This guide will help you deploy the Enterprise Data Platform to Render.com.

## Prerequisites

- GitHub account with the repository pushed
- Render.com account (https://dashboard.render.com/)

## Deployment Steps

### Method 1: Using Render Blueprint (Recommended)

1. **Push to GitHub** (if not already done):
   ```bash
   git add .
   git commit -m "Add Render deployment configuration"
   git push origin main
   ```

2. **Go to Render Dashboard**:
   - Visit: https://dashboard.render.com/
   - Click **"New +"** → **"Blueprint"**

3. **Connect Repository**:
   - Select: `princedubey45/Real-Time-Data-Lakehouse-with-Predictive-Analytics`
   - Render will automatically detect `render.yaml`

4. **Review Services**:
   - PostgreSQL Warehouse
   - MinIO Object Storage
   - Redis Cache
   - Airflow Webserver
   - UI Dashboard (Static Site)

5. **Click "Apply"** and wait for deployment (5-10 minutes)

---

### Method 2: Manual Service Creation

If Blueprint doesn't work, create services manually:

#### 1. PostgreSQL Database

1. Click **"New +"** → **"PostgreSQL"**
2. Configure:
   - **Name:** `postgres-warehouse`
   - **Database:** `data_warehouse`
   - **User:** `warehouse`
   - **Region:** Choose closest to you
   - **Plan:** Free
3. Click **"Create Database"**
4. Save the **Internal Database URL** (you'll need it)

#### 2. Redis

1. Click **"New +"** → **"Redis"**
2. Configure:
   - **Name:** `redis`
   - **Plan:** Free
3. Click **"Create Redis"**

#### 3. MinIO (Web Service)

1. Click **"New +"** → **"Web Service"**
2. Configure:
   - **Name:** `minio`
   - **Environment:** Docker
   - **Repository:** Select your GitHub repo
   - **Dockerfile Path:** `./Dockerfile.minio`
   - **Plan:** Free
3. Add Environment Variables:
   - `MINIO_ROOT_USER` = `minioadmin`
   - `MINIO_ROOT_PASSWORD` = `minioadmin123`
4. Click **"Create Web Service"**

#### 4. Airflow Webserver

1. Click **"New +"** → **"Web Service"**
2. Configure:
   - **Name:** `airflow-webserver`
   - **Environment:** Docker
   - **Repository:** Select your GitHub repo
   - **Dockerfile Path:** `./Dockerfile.airflow`
   - **Plan:** Starter ($7/month) - Free tier won't work for Airflow
3. Add Environment Variables:
   - `AIRFLOW__CORE__EXECUTOR` = `LocalExecutor`
   - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` = (paste PostgreSQL Internal URL)
   - `AIRFLOW__CORE__FERNET_KEY` = (generate random 32-char string)
   - `AIRFLOW__WEBSERVER__SECRET_KEY` = (generate random string)
   - `AIRFLOW__CORE__LOAD_EXAMPLES` = `false`
4. Click **"Create Web Service"**

#### 5. UI Dashboard (Static Site)

1. Click **"New +"** → **"Static Site"**
2. Configure:
   - **Name:** `ui-dashboard`
   - **Repository:** Select your GitHub repo
   - **Build Command:** (leave empty)
   - **Publish Directory:** `ui`
3. Click **"Create Static Site"**

---

## Access Your Deployed Services

After deployment completes, you'll get URLs like:

- **UI Dashboard:** `https://ui-dashboard.onrender.com`
- **Airflow:** `https://airflow-webserver.onrender.com` (admin/admin)
- **MinIO Console:** `https://minio.onrender.com` (minioadmin/minioadmin123)
- **PostgreSQL:** Internal URL only (not publicly accessible)

---

## Important Notes

### Free Tier Limitations

⚠️ **Render Free Tier:**
- Services spin down after 15 minutes of inactivity
- 750 hours/month total across all services
- Limited CPU and memory
- Databases limited to 1GB storage

### Recommended Plans

For production use:
- **PostgreSQL:** Starter ($7/month) - 1GB RAM, 10GB storage
- **Airflow:** Starter ($7/month) - 512MB RAM
- **MinIO:** Starter ($7/month) - 512MB RAM
- **UI:** Free (static sites are always free)
- **Redis:** Free tier is sufficient

**Total Cost:** ~$21/month for full stack

---

## Troubleshooting

### Services Won't Start

1. Check logs in Render dashboard
2. Verify environment variables are set correctly
3. Ensure Dockerfile paths are correct

### Database Connection Issues

1. Use **Internal Database URL** (not External)
2. Format: `postgresql://user:password@host:5432/database`
3. Check PostgreSQL service is running

### Airflow Won't Initialize

1. Increase to Starter plan (512MB RAM minimum)
2. Check database connection string
3. View logs for specific errors

### MinIO Storage Issues

1. Add persistent disk in Render dashboard
2. Mount path: `/data`
3. Size: 1GB minimum

---

## Cost Optimization

### Option 1: Free Tier Only (Limited)
- Deploy only UI Dashboard (static site) - **FREE**
- Use simulated data in the UI
- Run backend locally when needed

### Option 2: Hybrid (Recommended)
- UI on Render (free)
- Backend on local Docker
- Best for development/testing

### Option 3: Full Cloud (Production)
- All services on Render (~$21/month)
- Always available
- Scalable

---

## Alternative: Deploy Backend Locally, UI on Render

1. Deploy only UI to Render (free)
2. Run backend locally:
   ```bash
   docker-compose up -d
   ```
3. Access:
   - UI: `https://ui-dashboard.onrender.com`
   - Backend: `http://localhost:8080` (Airflow)

---

## Next Steps After Deployment

1. **Access Airflow:** Enable the DAGs
2. **Access MinIO:** Create the `enterprise-lake` bucket
3. **Run Pipeline:** Trigger `etl_pipeline` DAG
4. **View Results:** Check UI dashboard

---

## Support

If you encounter issues:
1. Check Render logs
2. Review environment variables
3. Verify Dockerfile syntax
4. Check GitHub repository is up to date

---

## Estimated Deployment Time

- Blueprint deployment: 5-10 minutes
- Manual deployment: 15-20 minutes
- First-time builds: May take longer

**Good luck with your deployment!** 🚀
