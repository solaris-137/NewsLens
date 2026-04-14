# Apple Sentiment

## Verification Checklist

1. `cp .env.example .env`
2. `docker-compose up --build`
3. Confirm postgres is reachable at `localhost:5432`
4. Confirm redis is reachable at `localhost:6379`
5. Confirm api stub is reachable at [http://localhost:8000](http://localhost:8000)
6. Confirm dashboard stub is reachable at [http://localhost:3000](http://localhost:3000)
7. Push to `main` and confirm GitHub Actions CI runs all 4 jobs green
