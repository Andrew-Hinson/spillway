# Spillway local environment.
#
#   make up          create the kind cluster, install Kafka, Loki and Grafana,
#                    deploy the feeders and the Vector aggregator
#                    (pinned kind, kubectl, helm in ./bin)
#   make down        delete the cluster and its kubeconfig
#   make clean       make down, then remove ./bin
#   make grafana-ui  port-forward Grafana to localhost:3000
#   make test        run Go unit tests (needs Go on the host)
#   make lint        lint the Go code (pinned golangci-lint in ./bin)
#   make vector-check validate and unit-test the Vector config (in Docker)
#   make baseline    measure volume and latency over the last 15m (needs python3)
#
# Only Docker, curl and make are needed on the host. The cluster's kubeconfig is
# written to ./.kubeconfig so your ~/.kube/config is never touched:
#   export KUBECONFIG=$(make -s kubeconfig)

SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

CLUSTER_NAME    ?= spillway
KIND_CONFIG     := deploy/kind/cluster.yaml
KUBECONFIG_PATH := $(CURDIR)/.kubeconfig

KIND_VERSION    := v0.33.0
KUBECTL_VERSION := v1.37.1
HELM_VERSION    := v4.3.0
GOLANGCI_LINT_VERSION := v2.14.0

# Platform charts.
STRIMZI_CHART_VERSION := 1.2.0
LOKI_CHART_VERSION    := 18.13.7
GRAFANA_CHART_VERSION := 13.2.7
PROMETHEUS_CHART_VERSION := 29.35.0
VECTOR_CHART_VERSION  := 0.58.0
VECTOR_IMAGE          := timberio/vector:0.58.0-distroless-libc
GRAFANA_CHARTS        := https://grafana-community.github.io/helm-charts
PLATFORM              := deploy/platform
HELM_INSTALL           = $(HELM) upgrade --install --wait --timeout 10m

# Feeder images are tagged with a hash of their source, so the Deployment only
# rolls when the code changes.
WIKIMEDIA_IMAGE := spillway/wikimedia-feeder
WIKIMEDIA_TAG    = $(shell cat go.mod go.sum $$(find feeders/wikimedia -type f | sort) | sha256sum | cut -c1-12)
# Must be one of the images built for KIND_VERSION, pinned by digest (see kind release notes).
KIND_NODE_IMAGE := kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5

BIN     := $(CURDIR)/bin
KIND    := $(BIN)/kind-$(KIND_VERSION)
KUBECTL := $(BIN)/kubectl-$(KUBECTL_VERSION)
HELM    := $(BIN)/helm-$(HELM_VERSION)
GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)

OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

export KUBECONFIG := $(KUBECONFIG_PATH)
# Keep helm's config, cache and data out of $$HOME.
export HELM_CONFIG_HOME := $(BIN)/.helm/config
export HELM_CACHE_HOME  := $(BIN)/.helm/cache
export HELM_DATA_HOME   := $(BIN)/.helm/data

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: cluster platform feeders aggregator ## Bring the whole local stack up
	@echo "Spillway is up. export KUBECONFIG=$(KUBECONFIG_PATH)"

.PHONY: down
down: docker-check $(KIND) ## Tear the local stack down, leaving nothing behind
	$(KIND) delete cluster --name $(CLUSTER_NAME) --kubeconfig $(KUBECONFIG_PATH)
	rm -f $(KUBECONFIG_PATH)
	@# kind leaves its shared "kind" network behind; drop it once no kind clusters remain.
	@if [ -z "$$($(KIND) get clusters 2>/dev/null)" ] && docker network inspect kind >/dev/null 2>&1; then \
		echo "removing docker network 'kind'"; docker network rm kind >/dev/null; \
	fi

.PHONY: clean
clean: down ## make down, then remove downloaded tools
	rm -rf $(BIN)

.PHONY: cluster
cluster: docker-check $(KIND) $(KUBECTL) ## Create the kind cluster (idempotent) and wait for nodes
	@if $(KIND) get clusters | grep -qx '$(CLUSTER_NAME)'; then \
		echo "kind cluster '$(CLUSTER_NAME)' already exists"; \
		$(KIND) export kubeconfig --name $(CLUSTER_NAME) --kubeconfig $(KUBECONFIG_PATH); \
	else \
		$(KIND) create cluster --name $(CLUSTER_NAME) --config $(KIND_CONFIG) \
			--image $(KIND_NODE_IMAGE) --kubeconfig $(KUBECONFIG_PATH) --wait 120s; \
	fi
	$(KUBECTL) wait --for=condition=Ready nodes --all --timeout=120s
	$(KUBECTL) get nodes -o wide

.PHONY: platform
platform: kafka loki prometheus grafana ## Install Kafka, Loki, Prometheus and Grafana (idempotent)

.PHONY: kafka
kafka: $(HELM) $(KUBECTL) ## Install the Strimzi operator and a single-node Kafka cluster
	$(HELM_INSTALL) strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
		--version $(STRIMZI_CHART_VERSION) --namespace kafka --create-namespace \
		-f $(PLATFORM)/strimzi/values.yaml
	$(KUBECTL) apply -f $(PLATFORM)/kafka/
	$(KUBECTL) -n kafka wait kafka/spillway --for=condition=Ready --timeout=10m

.PHONY: loki
loki: $(HELM) ## Install Loki (monolithic, filesystem storage)
	$(HELM_INSTALL) loki loki --repo $(GRAFANA_CHARTS) \
		--version $(LOKI_CHART_VERSION) --namespace observability --create-namespace \
		-f $(PLATFORM)/loki/values.yaml

.PHONY: prometheus
prometheus: $(HELM) ## Install Prometheus (scrapes pods annotated prometheus.io/scrape)
	$(HELM_INSTALL) prometheus prometheus --repo https://prometheus-community.github.io/helm-charts \
		--version $(PROMETHEUS_CHART_VERSION) --namespace observability --create-namespace \
		-f $(PLATFORM)/prometheus/values.yaml

.PHONY: grafana
grafana: $(HELM) $(KUBECTL) ## Install Grafana with Loki and Prometheus datasources and the dashboards
	$(KUBECTL) create namespace observability --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n observability create configmap grafana-dashboards \
		--from-file=dashboards/ --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(HELM_INSTALL) grafana grafana --repo $(GRAFANA_CHARTS) \
		--version $(GRAFANA_CHART_VERSION) --namespace observability --create-namespace \
		-f $(PLATFORM)/grafana/values.yaml

.PHONY: feeders
feeders: wikimedia-feeder ## Build and deploy the feeders

.PHONY: wikimedia-feeder
wikimedia-feeder: docker-check $(KIND) $(KUBECTL) ## Build, load and deploy the Wikimedia feeder
	docker build -q -t $(WIKIMEDIA_IMAGE):$(WIKIMEDIA_TAG) -f feeders/wikimedia/Dockerfile .
	$(KIND) load docker-image $(WIKIMEDIA_IMAGE):$(WIKIMEDIA_TAG) --name $(CLUSTER_NAME)
	sed 's|$(WIKIMEDIA_IMAGE):dev|$(WIKIMEDIA_IMAGE):$(WIKIMEDIA_TAG)|' deploy/feeders/wikimedia.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n kafka wait kafkatopic/wikimedia.recentchange --for=condition=Ready --timeout=2m
	$(KUBECTL) -n feeders rollout status deployment/wikimedia-feeder --timeout=3m

.PHONY: aggregator
aggregator: $(HELM) $(KUBECTL) ## Deploy the Vector aggregator (Kafka -> Loki)
	$(KUBECTL) create namespace vector --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n vector create configmap vector-aggregator-config \
		--from-file=vector/aggregator/vector.yaml --dry-run=client -o yaml | $(KUBECTL) apply -f -
	@# The config hash annotation rolls the pod when vector.yaml changes.
	$(HELM_INSTALL) vector-aggregator vector --repo https://helm.vector.dev \
		--version $(VECTOR_CHART_VERSION) --namespace vector \
		-f vector/aggregator/values.yaml \
		--set-string 'podAnnotations.checksum/config=$(shell sha256sum vector/aggregator/vector.yaml | cut -c1-12)'

.PHONY: vector-check
vector-check: docker-check ## Validate and unit-test the Vector config
	docker run --rm -v $(CURDIR)/vector:/vector:ro $(VECTOR_IMAGE) \
		validate --no-environment /vector/aggregator/vector.yaml
	docker run --rm -v $(CURDIR)/vector:/vector:ro $(VECTOR_IMAGE) \
		test /vector/aggregator/vector.yaml /vector/tests/aggregator.yaml

.PHONY: baseline
baseline: $(KUBECTL) ## Print volume and latency over the last WINDOW (default 15m) as Markdown
	@$(KUBECTL) -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf=$$!; \
		trap 'kill $$pf' EXIT; sleep 2; \
		python3 bench/baseline.py --window $(or $(WINDOW),15m)

.PHONY: test
test: ## Run Go unit tests
	go test -race ./...

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Lint the Go code
	$(GOLANGCI_LINT) run ./...

.PHONY: grafana-ui
grafana-ui: $(KUBECTL) ## Port-forward Grafana to http://localhost:3000 and print the login
	@echo "Grafana: http://localhost:3000  user: admin  password: $$($(KUBECTL) -n observability \
		get secret grafana -o jsonpath='{.data.admin-password}' | base64 -d)"
	$(KUBECTL) -n observability port-forward svc/grafana 3000:80

.PHONY: kubeconfig
kubeconfig: ## Print the path to the cluster kubeconfig
	@echo $(KUBECONFIG_PATH)

.PHONY: docker-check
docker-check:
	@docker info >/dev/null 2>&1 || { \
		echo "error: cannot reach the Docker daemon. Is it running, and can $$USER use it (docker group)?" >&2; \
		exit 1; }

.PHONY: tools
tools: $(KIND) $(KUBECTL) $(HELM) $(GOLANGCI_LINT) ## Install pinned kind, kubectl, helm and golangci-lint into ./bin

$(KIND):
	@mkdir -p $(BIN)
	curl -fsSLo $@ https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-$(OS)-$(ARCH)
	chmod +x $@
	ln -sf $(notdir $@) $(BIN)/kind

$(KUBECTL):
	@mkdir -p $(BIN)
	curl -fsSLo $@ https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(OS)/$(ARCH)/kubectl
	chmod +x $@
	ln -sf $(notdir $@) $(BIN)/kubectl

$(HELM):
	@mkdir -p $(BIN)
	curl -fsSL https://get.helm.sh/helm-$(HELM_VERSION)-$(OS)-$(ARCH).tar.gz | tar -xzO $(OS)-$(ARCH)/helm > $@
	chmod +x $@
	ln -sf $(notdir $@) $(BIN)/helm

$(GOLANGCI_LINT):
	@mkdir -p $(BIN)
	curl -fsSL https://github.com/golangci/golangci-lint/releases/download/$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION:v%=%)-$(OS)-$(ARCH).tar.gz \
		| tar -xzO golangci-lint-$(GOLANGCI_LINT_VERSION:v%=%)-$(OS)-$(ARCH)/golangci-lint > $@
	chmod +x $@
	ln -sf $(notdir $@) $(BIN)/golangci-lint
