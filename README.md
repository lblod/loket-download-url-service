# loket-download-url-service
A microservice that periodically looks in the database for urls associated with inzending-voor-toezicht objects and tries to download and store their content locally, if not already stored. You can force it to run immediately by visiting /checkurls subroute. 
```
  FILE_STORAGE
    The local storage of files

  CACHING_MAX_RETRIES
    How many times will the service try to download a resource before considering it as failed.

  CACHING_CRON_PATTERN
    The time interval of service's re-execution.
```
## Installation
To add the service to your stack, add the following snippet to docker-compose.yml:

```
download:
    image: lblod/download-url-service:0.0.1
    volumes:
      - ./data/files:/share
    restart: always
    logging: *default-logging
    environment:
      CRON_PATTERN: "* */15 * * * *" # run every quarter
```
### Environment variables
```
  NODE_ENV: "development"
  CACHING_MAX_RETRIES: 300
  CACHING_CRON_PATTERN: '* */15 * * * *'
  FILE_STORAGE: '/data/files'
  MAX_PENDING_TIME_IN_SECONDS: 3600
```
