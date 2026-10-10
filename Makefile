# Spillway local environment.
#
#   make up          create the kind cluster, install Kafka, Loki and Grafana,
#                    deploy the feeders, the Vector aggregator, the agents and
#                    the operator
#                    (pinned kind, kubectl, helm in ./bin)
#   make down        delete the cluster and its kubeconfig
#   make clean       make down, then remove ./bin
#   make grafana-ui  port-forward Grafana to localhost:3000
#   make test        run Go unit tests, CRD validation against a local API server
#                    (needs Go on the host)
#   make generate    regenerate deepcopy code, the CRD and operator RBAC
#   make spillwayctl build the CLI that validates and renders specs offline
#   make lint        lint the Go code (pinned golangci-lint in ./bin)
#   make vector-check validate and unit-test the Vector config (in Docker)
#   make baseline    measure volume and latency over the last 15m (needs python3)
#   make leak-check  search the hot sink for injected fixture values (needs python3)
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
CONTROLLER_GEN_VERSION := v0.22.0
SETUP_ENVTEST_VERSION  := v0.25.2
# Kubernetes version of the API server the CRD validation tests run against;
# matches the kind node image.
ENVTEST_K8S_VERSION    := 1.37.0

# Platform charts.
STRIMZI_CHART_VERSION := 1.2.0
LOKI_CHART_VERSION    := 18.13.7
GRAFANA_CHART_VERSION := 13.2.7
PROMETHEUS_CHART_VERSION := 29.35.0
VECTOR_CHART_VERSION  := 0.58.0
VECTOR_VERSION        := 0.58.0
VECTOR_IMAGE          := timberio/vector:$(VECTOR_VERSION)-distroless-libc
GRAFANA_CHARTS        := https://grafana-community.github.io/helm-charts
PLATFORM              := deploy/platform
HELM_INSTALL           = $(HELM) upgrade --install --wait --timeout 10m

# Feeder images are tagged with a hash of their source, so the Deployment only
# rolls when the code changes.
WIKIMEDIA_IMAGE := spillway/wikimedia-feeder
WIKIMEDIA_TAG    = $(shell cat go.mod go.sum $$(find feeders/wikimedia -type f | sort) | sha256sum | cut -c1-12)
OPERATOR_IMAGE  := spillway/operator
OPERATOR_TAG     = $(shell (echo $(VECTOR_VERSION); cat go.mod go.sum $$(find api cmd/operator internal -type f | sort)) | sha256sum | cut -c1-12)
# Must be one of the images built for KIND_VERSION, pinned by digest (see kind release notes).
KIND_NODE_IMAGE := kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5

BIN     := $(CURDIR)/bin
KIND    := $(BIN)/kind-$(KIND_VERSION)
KUBECTL := $(BIN)/kubectl-$(KUBECTL_VERSION)
HELM    := $(BIN)/helm-$(HELM_VERSION)
GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
CONTROLLER_GEN := $(BIN)/controller-gen-$(CONTROLLER_GEN_VERSION)
SETUP_ENVTEST  := $(BIN)/setup-envtest-$(SETUP_ENVTEST_VERSION)
# Vector for the validation tests, the same version the aggregator and the
# operator image run. Release builds: macOS on arm64, static Linux builds.
VECTOR         := $(BIN)/vector-$(VECTOR_VERSION)
VECTOR_TRIPLE   = $(if $(filter darwin,$(OS)),arm64-apple-darwin,$(if $(filter arm64,$(ARCH)),aarch64,x86_64)-unknown-linux-musl)

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
up: cluster platform feeders aggregator agent operator ## Bring the whole local stack up
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

FIXTURES_IMAGE  := spillway/fixtures
FIXTURES_TAG     = $(shell cat go.mod go.sum $$(find feeders/fixtures -type f | sort) | sha256sum | cut -c1-12)

.PHONY: fixtures
fixtures: docker-check $(KIND) $(KUBECTL) ## Inject known fake PII into the live streams (opt-in; RATE=fixtures/s, default 1)
	docker build -q -t $(FIXTURES_IMAGE):$(FIXTURES_TAG) -f feeders/fixtures/Dockerfile .
	$(KIND) load docker-image $(FIXTURES_IMAGE):$(FIXTURES_TAG) --name $(CLUSTER_NAME)
	sed -e 's|$(FIXTURES_IMAGE):dev|$(FIXTURES_IMAGE):$(FIXTURES_TAG)|' \
		-e 's|value: "1"   # fixtures per second|value: "$(or $(RATE),1)"   # fixtures per second|' \
		deploy/feeders/fixtures.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n fixtures rollout status deployment/fixture-injector --timeout=3m

.PHONY: aggregator
aggregator: $(HELM) $(KUBECTL) ## Deploy the Vector aggregator (Kafka -> Loki)
	$(KUBECTL) create namespace vector --dry-run=client -o yaml | $(KUBECTL) apply -f -
	@# Bootstrap config only: once running, the operator owns this ConfigMap and
	@# renders it from the LogPipelines, so an existing one is left alone.
	$(KUBECTL) -n vector get configmap vector-aggregator-config >/dev/null 2>&1 || \
		$(KUBECTL) -n vector create configmap vector-aggregator-config --from-file=vector/aggregator/vector.yaml
	$(HELM_INSTALL) vector-aggregator vector --repo https://helm.vector.dev \
		--version $(VECTOR_CHART_VERSION) --namespace vector \
		-f vector/aggregator/values.yaml

.PHONY: agent
agent: $(HELM) $(KUBECTL) ## Deploy the Vector agent DaemonSet (pod logs -> aggregator)
	$(KUBECTL) create namespace vector --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n vector create configmap vector-agent-config \
		--from-file=vector/agent/vector.yaml --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(HELM_INSTALL) vector-agent vector --repo https://helm.vector.dev \
		--version $(VECTOR_CHART_VERSION) --namespace vector \
		-f vector/agent/values.yaml \
		--set-string 'podAnnotations.checksum/config=$(shell sha256sum vector/agent/vector.yaml | cut -c1-12)'

.PHONY: operator
operator: docker-check $(KIND) $(KUBECTL) ## Build, load and deploy the operator and the LogPipeline CRD
	docker build -q -t $(OPERATOR_IMAGE):$(OPERATOR_TAG) --build-arg VECTOR_VERSION=$(VECTOR_VERSION) -f cmd/operator/Dockerfile .
	$(KIND) load docker-image $(OPERATOR_IMAGE):$(OPERATOR_TAG) --name $(CLUSTER_NAME)
	$(KUBECTL) apply --server-side -f deploy/operator/crd
	$(KUBECTL) wait crd/logpipelines.spillway.dev --for=condition=Established --timeout=1m
	$(KUBECTL) create namespace vector --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -f deploy/operator/rbac
	sed 's|$(OPERATOR_IMAGE):dev|$(OPERATOR_IMAGE):$(OPERATOR_TAG)|' deploy/operator/operator.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n spillway-system rollout status deployment/spillway-operator --timeout=3m

.PHONY: vector-check
vector-check: docker-check ## Validate and unit-test the Vector configs, including every rendered example
	docker run --rm -v $(CURDIR)/vector:/vector:ro $(VECTOR_IMAGE) \
		validate --no-environment /vector/aggregator/vector.yaml
	docker run --rm -v $(CURDIR)/vector:/vector:ro -e VECTOR_SELF_NODE_NAME=ci $(VECTOR_IMAGE) \
		validate --no-environment /vector/agent/vector.yaml
	docker run --rm -v $(CURDIR)/vector:/vector:ro $(VECTOR_IMAGE) \
		test /vector/aggregator/vector.yaml /vector/tests/aggregator.yaml
	@# The renderer's golden files: every example spec must render a valid config
	@# that passes the unit tests generated for it.
	for f in $$(ls internal/render/testdata/*.yaml | grep -v '\.tests\.yaml$$'); do \
		name=$$(basename $$f .yaml); \
		echo "validate and test $$f"; \
		docker run --rm -v $(CURDIR)/internal/render/testdata:/rendered:ro $(VECTOR_IMAGE) \
			validate --no-environment /rendered/$$name.yaml || exit 1; \
		docker run --rm -v $(CURDIR)/internal/render/testdata:/rendered:ro $(VECTOR_IMAGE) \
			test /rendered/$$name.yaml /rendered/$$name.tests.yaml || exit 1; \
	done

.PHONY: baseline
baseline: $(KUBECTL) ## Print volume and latency over the last WINDOW (default 15m) as Markdown
	@$(KUBECTL) -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf=$$!; \
		trap 'kill $$pf' EXIT; sleep 2; \
		python3 bench/baseline.py --window $(or $(WINDOW),15m)

.PHONY: leak-check
leak-check: $(KUBECTL) ## Search the hot sink for injected fixture values over WINDOW (default 15m); fails on any leak
	@$(KUBECTL) -n observability port-forward svc/loki 3100:3100 >/dev/null 2>&1 & pf1=$$!; \
		$(KUBECTL) -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf2=$$!; \
		trap 'kill $$pf1 $$pf2' EXIT; sleep 2; \
		python3 bench/leakcheck.py --window $(or $(WINDOW),15m)

.PHONY: sampling-check
sampling-check: $(KUBECTL) ## Report each team's dedupe, sampling and budget drops over WINDOW (default 10m); REF=team=ref compares levels to an unsampled team
	@$(KUBECTL) -n observability port-forward svc/loki 3100:3100 >/dev/null 2>&1 & pf1=$$!; \
		$(KUBECTL) -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf2=$$!; \
		trap 'kill $$pf1 $$pf2' EXIT; sleep 2; \
		python3 bench/sampling.py --kubectl $(KUBECTL) --window $(or $(WINDOW),10m) $(if $(REF),--ref $(REF))

.PHONY: redaction-fp
redaction-fp: $(VECTOR) ## Measure redaction false positives on DURATION seconds (default 600) of the live Wikimedia stream
	@mkdir -p bin/samples
	curl -sN --max-time $(or $(DURATION),600) -A "spillway-redactfp/0.1 (https://github.com/Andrew-Hinson/spillway)" \
		https://stream.wikimedia.org/v2/stream/recentchange | sed -un 's/^data: //p' > bin/samples/recentchange.ndjson || true
	go run ./bench/redactfp -vector-bin $(VECTOR) < bin/samples/recentchange.ndjson

.PHONY: m2-gate
m2-gate: $(KUBECTL) spillwayctl ## Run the M2 gate demo against a fresh `make up` (see docs/results/m2-gate.md)
	bench/m2_gate.sh

.PHONY: test
test: $(SETUP_ENVTEST) $(VECTOR) ## Run Go unit tests, including CRD validation against a local API server
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(BIN)/envtest -p path)" \
		VECTOR_BIN=$(VECTOR) go test -race ./...

.PHONY: spillwayctl
spillwayctl: $(VECTOR) ## Build the spillwayctl CLI into ./bin (validate and render specs without a cluster)
	go build -o $(BIN)/spillwayctl ./cmd/spillwayctl
	@echo "$(BIN)/spillwayctl validate --vector-bin $(VECTOR) examples/"

.PHONY: generate
generate: $(CONTROLLER_GEN) ## Regenerate deepcopy code, the CRD and operator RBAC from the Go types
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd rbac:roleName=spillway-operator paths=./api/... paths=./internal/... \
		output:crd:artifacts:config=deploy/operator/crd output:rbac:artifacts:config=deploy/operator/rbac

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
tools: $(KIND) $(KUBECTL) $(HELM) $(GOLANGCI_LINT) $(CONTROLLER_GEN) $(SETUP_ENVTEST) $(VECTOR) ## Install the pinned tools into ./bin

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

$(CONTROLLER_GEN):
	@mkdir -p $(BIN)
	GOBIN=$(BIN)/.gobin go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
	mv $(BIN)/.gobin/controller-gen $@
	ln -sf $(notdir $@) $(BIN)/controller-gen

$(SETUP_ENVTEST):
	@mkdir -p $(BIN)
	GOBIN=$(BIN)/.gobin go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)
	mv $(BIN)/.gobin/setup-envtest $@
	ln -sf $(notdir $@) $(BIN)/setup-envtest

$(VECTOR):
	@mkdir -p $(BIN)
	curl -fsSL https://packages.timber.io/vector/$(VECTOR_VERSION)/vector-$(VECTOR_VERSION)-$(VECTOR_TRIPLE).tar.gz \
		| tar -xzO ./vector-$(VECTOR_TRIPLE)/bin/vector > $@
	chmod +x $@
	ln -sf $(notdir $@) $(BIN)/vector
