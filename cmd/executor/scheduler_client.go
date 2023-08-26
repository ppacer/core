package main

import "net/http"

type SchedulerClient struct {
	httpClient   *http.Client
	schedulerUrl string
}
