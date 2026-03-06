# Step 1 - File processing event serializer


## What

This step takes as input a storage endpoint or path and generates, for each one, and event next step queue.

The output event payload for each even is a JSON structure containing:

payload-version: string
file-name: string
source-provider: string


## How

This implements a single process that recieve as parameter from an input queue named: pipeline-trigger a JSON Payload witht the fields:

storage-type: string
storge-path: string

Connects to the source storage, list the files there and for each one will generate an event in the output queue: "normalizer-queue"





