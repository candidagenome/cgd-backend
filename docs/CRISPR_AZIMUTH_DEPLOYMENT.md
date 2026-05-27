# CRISPR Azimuth Efficiency Model - Production Deployment Guide

## Overview

This document describes the deployment steps for the CRISPR Guide Designer's Azimuth efficiency model (Doench 2016 Rule Set 2) implementation.

**Branch**: `redmine_79_to_85`
**Release Date**: May 2025
**Impact**: CRISPR guide efficiency scoring and ranking

---

## Summary of Changes

### Backend (`cgd-backend`)

| File | Change Type | Description |
|------|-------------|-------------|
| `cgd/api/services/azimuth_minimal.py` | **NEW** | Coefficient-based Azimuth model implementation |
| `cgd/api/services/crispr_service.py` | Modified | Integrated Azimuth scoring, tuned ranking penalty |
| `scripts/crispor_benchmark/README.md` | Modified | Updated benchmark documentation |

### Frontend (`cgd-frontend`)

| File | Change Type | Description |
|------|-------------|-------------|
| `src/pages/help/CrisprGuideFinderHelp.jsx` | Modified | Added Azimuth model and benchmark documentation |

---

## Pre-Deployment Checklist

### 1. Dependencies

**No new dependencies required.**

The Azimuth implementation uses only Python standard library modules:
- `re` (regex)
- `logging`
- `typing`
- `math` (for exponential position bonus)

Verify existing dependencies are installed:
```bash
pip install -r requirements.txt
```

### 2. Database Changes

**No database changes required.**

The efficiency scoring is computed on-the-fly from sequence data.

### 3. Configuration Changes

**No configuration changes required.**

The model coefficients are embedded in the code (`azimuth_minimal.py`).

### 4. External Services

**No new external services required.**

- BLAST off-target search: unchanged
- JBrowse integration: unchanged

---

## Deployment Steps

### Backend Deployment

```bash
# 1. SSH to production server
ssh cgd-backend-prod

# 2. Navigate to backend directory
cd /path/to/cgd-backend

# 3. Pull latest changes
git fetch origin
git checkout redmine_79_to_85
git pull origin redmine_79_to_85

# 4. Verify new file exists
ls -la cgd/api/services/azimuth_minimal.py

# 5. Run tests to verify
source venv/bin/activate
pytest tests/api/test_crispr_service.py -v

# 6. Restart the backend service
sudo systemctl restart cgd-api
# OR if using gunicorn directly:
# sudo systemctl restart gunicorn

# 7. Verify service is running
sudo systemctl status cgd-api
curl http://localhost:8000/health
```

### Frontend Deployment

```bash
# 1. SSH to frontend server (if separate)
ssh cgd-frontend-prod

# 2. Navigate to frontend directory
cd /path/to/cgd-frontend

# 3. Pull latest changes
git fetch origin
git checkout redmine_79_to_85
git pull origin redmine_79_to_85

# 4. Build the frontend
npm install
npm run build

# 5. Deploy build artifacts
# (depends on your deployment setup - nginx, S3, etc.)
cp -r build/* /var/www/cgd/

# 6. Verify deployment
curl https://www.candidagenome.org/help/crispr
```

---

## Post-Deployment Verification

### 1. Smoke Tests

Test the CRISPR designer with known genes:

```bash
# Test API directly
curl -X POST "https://api.candidagenome.org/api/crispr/design" \
  -H "Content-Type: application/json" \
  -d '{
    "gene_name": "ALS1",
    "organism": "C_albicans_SC5314_A22",
    "pam_type": "NGG",
    "target_region": "5_prime",
    "max_guides": 10,
    "check_offtargets": false
  }'
```

### 2. Verify Efficiency Scores

Check that efficiency scores are in expected ranges (0-100):
- Most guides should have scores between 30-70
- Very high (>80) or very low (<20) scores should be rare

### 3. Verify Ranking

Top-ranked guides should generally have:
- High efficiency scores (50+)
- Early positions in 5' region (for knockout targeting)
- Low off-target counts

### 4. Check Help Page

Verify the help documentation loads correctly:
- https://www.candidagenome.org/help/crispr
- Check "Scoring Methodology" section for Azimuth details
- Check "Benchmark Validation" section for comparison tables

---

## Rollback Plan

If issues are detected after deployment:

### Quick Rollback (Backend)

```bash
# 1. SSH to production
ssh cgd-backend-prod

# 2. Checkout previous commit
cd /path/to/cgd-backend
git checkout HEAD~1

# 3. Restart service
sudo systemctl restart cgd-api

# 4. Verify rollback
curl http://localhost:8000/health
```

### Full Rollback

```bash
# Revert to main branch
git checkout main
git pull origin main
sudo systemctl restart cgd-api
```

---

## Performance Notes

### Efficiency Prediction Performance

- **Azimuth prediction**: <1ms per guide (coefficient-based, no ML inference)
- **Total design request**: ~2-5 seconds (dominated by BLAST off-target search)
- **No memory increase**: Coefficients are small dictionaries in memory

### Benchmark Results

| Comparison | Match Rate |
|------------|------------|
| CGD → CHOPCHOP (top 10) | **55.7%** |
| CGD → CRISPOR (top 10) | 52.5% |
| CHOPCHOP → CRISPOR (top 10) | 54.3% |

CGD now matches CHOPCHOP rankings better than CRISPOR does.

---

## Troubleshooting

### Issue: Efficiency scores all showing 0 or missing

**Cause**: Azimuth module not loaded properly.

**Solution**:
```bash
# Check module exists
python -c "from cgd.api.services.azimuth_minimal import predict_efficiency; print('OK')"

# Check for import errors
python -c "from cgd.api.services import crispr_service; print('OK')"
```

### Issue: Guides ranked differently than expected

**Cause**: Position bonus or penalty weights may need adjustment.

**Solution**: Check `_calculate_chopchop_penalty()` in `crispr_service.py`:
- Efficiency weight: 25
- Position bonus: exponential decay formula
- GC penalty: 200

### Issue: Help page not showing new sections

**Cause**: Frontend build not deployed.

**Solution**: Rebuild and redeploy frontend:
```bash
npm run build
# Deploy build artifacts
```

---

## Files Reference

### New Files

```
cgd-backend/
└── cgd/api/services/
    └── azimuth_minimal.py    # 234 lines, coefficient-based Azimuth model
```

### Modified Files

```
cgd-backend/
├── cgd/api/services/
│   └── crispr_service.py     # Azimuth integration, penalty tuning
└── scripts/crispor_benchmark/
    └── README.md             # Updated benchmark results

cgd-frontend/
└── src/pages/help/
    └── CrisprGuideFinderHelp.jsx  # Azimuth & benchmark documentation
```

---

## Contact

For deployment issues, contact the CGD development team.

**Related Tickets**: Redmine #79-85
**Branch**: `redmine_79_to_85`
