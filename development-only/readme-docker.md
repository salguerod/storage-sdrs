# Docker
The SDRS server application is intended to run in a docker container.

## Build & Publish
The docker build process will copy source code, install dependencies, and run the maven package target. 
The docker image exposes port 8080 for the service.

The docker image can be published to the GCP container registry.

See https://cloud.google.com/container-registry/docs/pushing-and-pulling for more details.


## Configuration
There are configuration values for the application that can be passed in as environment variables. 
When running the docker image, the environment variables can be passed in as a file using a command like:

    docker run <image> --env-file=<path/to/txt/file>

## Credentials
Providing GCP credentials to the docker container is a 2 step process
1. Mount the directory containing the credentials JSON file on the docker container as a run argument

    `docker run <image> -v </directory/of/credentials/file>:</mounted/directory>`

2. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the location of the credentials file in the mounted 
directory.

    `GOOGLE_APPLICATION_CREDENTIALS=</mounted/directory/credentials.json>`
    
    This can be set within the environment variables file in the configuration step specified above.