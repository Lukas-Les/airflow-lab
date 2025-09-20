# airflow-lab

If this will be your first time running this:
`docker compose up airflow-init`

then 

Create utility program for your development environment to interact with localstack. Create a program at ~/.local/bin/awslocal with following content:
```bash
#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws --endpoint-url=http://127.0.0.1:4566 "$@"
```

Make sure it is executable by running chmod +x ~/.local/bin/awslocal

then

`docker compose up`

later on run just `docker compose up` to start up
