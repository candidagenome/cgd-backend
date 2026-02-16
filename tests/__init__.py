"""
CGD Backend Tests

This package contains all tests for the CGD backend application.

Test Organization:
- api/: Tests for FastAPI service layer (curation, query, export services)
- scripts/: Tests for backend scripts (cron jobs, data loading, utilities)
  - cron/: Scheduled job script tests
  - load/: Data loading script tests
  - check/: Data validation script tests
  - loading/: Loading utility tests
  - pathways/: Pathway Tools script tests
  - proteins/: Protein analysis script tests
  - utilities/: Standalone utility tests
  - And other categories...

Running Tests:
    # Run all tests
    pytest tests/

    # Run only API tests
    pytest tests/api/

    # Run only script tests
    pytest tests/scripts/

    # Run specific category
    pytest tests/scripts/cron/
    pytest tests/api/test_go_service.py

    # Run with coverage
    pytest --cov=cgd tests/
"""
