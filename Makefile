# Spillway local environment.
#
#   make up     create the kind cluster (installs pinned kind + kubectl into ./bin)
#   make down   delete the cluster and its kubeconfig
#   make clean  make down, then remove ./bin
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
# Must be one of the images built for KIND_VERSION, pinned by digest (see kind release notes).
KIND_NODE_IMAGE := kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5

BIN     := $(CURDIR)/bin
KIND    := $(BIN)/kind-$(KIND_VERSION)
KUBECTL := $(BIN)/kubectl-$(KUBECTL_VERSION)

OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

export KUBECONFIG := $(KUBECONFIG_PATH)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: cluster ## Bring the whole local stack up
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

.PHONY: kubeconfig
kubeconfig: ## Print the path to the cluster kubeconfig
	@echo $(KUBECONFIG_PATH)

.PHONY: docker-check
docker-check:
	@docker info >/dev/null 2>&1 || { \
		echo "error: cannot reach the Docker daemon. Is it running, and can $$USER use it (docker group)?" >&2; \
		exit 1; }

.PHONY: tools
tools: $(KIND) $(KUBECTL) ## Install pinned kind and kubectl into ./bin

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
