# Introduction

This goal of this project is to reproduce an issue regarding concurrent access to mongo.
The scenario is the following:
* a thread is updating a document's field, using the replaceOne method. This field is indexed.
* in parallel, another thread lists all documents, filtering on the indexed field.

## Expected result
The second thread should always get the same number of documents.
The updates should not have any impact on it, since the filtering criteria include the document before and after the update.

## Actual result
Sometimes, the updated document is not part of the resulting list.
Sometimes, the updated document is duplicated in the resulting list.

# How to build and run the test

## Pre-requisites
Docker, docker-compose, java 17, maven.

## Command
`mvn clean install`

## Concurrency integration test
The test first creates a collections "task" containing documents with the followings fields:
* name
* status (values: TODO, IN_PROGRESS, DONE)

Then, we start 2 parallel threads:
* One updates a task from status TODO to IN_PROGRESS, then from status IN_PROGRESS to TODO, in a loop.
* The other one lists all undone tasks, filtering out those in status DONE.

As soon as we detect that the list of undone tasks is wrong, the test is marked as failed.

## Constants
The following constants can be modified to experiment, in ConcurrencyIT.java:
* UPDATE_SLEEP_TIME_MILLISECONDS: time to wait between each update command
* LIST_SLEEP_TIME_MILLISECONDS: time to wait between each list command
* DOCUMENT_CREATION_ITERATION_COUNT: number of documents of each status to create
