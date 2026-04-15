# Azure Alert Rules - Manual Setup

After the pipeline has been running for at least 1 hour so the metric exists:

## Alert: Pipeline Stalled
1. Azure Portal -> your App Insights resource -> Alerts -> + New alert rule
2. Condition: Custom metric - `articles_processed_total`
3. Aggregation: Count
4. Operator: Less than
5. Threshold: 1
6. Evaluation period: 2 hours
7. Frequency: 30 minutes
8. Action group: Create new -> Email/SMS -> enter `ALERT_EMAIL`
9. Alert name: `Apple Sentiment - Pipeline Stalled`
10. Severity: 2 (Warning)

## Alert: High Error Rate
1. Azure Portal -> your App Insights resource -> Alerts -> + New alert rule
2. Condition: `exceptions/count > 10` in 1 hour
3. Action group: Create new -> Email/SMS -> enter `ALERT_EMAIL`
4. Alert name: `Apple Sentiment - High Error Rate`
5. Severity: 1 (Critical)
