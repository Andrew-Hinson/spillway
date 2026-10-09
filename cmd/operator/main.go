// Command operator runs the Spillway operator, which turns LogPipeline
// resources into Vector pipeline config.
package main

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/controller"
	"github.com/Andrew-Hinson/spillway/internal/render"
	"github.com/Andrew-Hinson/spillway/internal/validate"
)

func main() {
	var metricsAddr, probeAddr string
	var leaderElect bool
	rec := controller.AggregatorReconciler{Options: render.DefaultOptions()}
	// Cold storage (MinIO) isn't deployed until M3.5: until it's configured,
	// pipelines that route to cold are marked Invalid rather than rendered
	// with a sink that would block once its buffer filled.
	rec.Options.Cold = nil
	var cold render.ColdStorage
	var vector validate.Vector
	flag.StringVar(&vector.Bin, "vector-bin", "/usr/local/bin/vector", "vector binary used to validate rendered config (same version as the aggregator)")
	flag.DurationVar(&vector.Timeout, "validation-timeout", 30*time.Second, "how long `vector validate` may take")
	// Canary rollouts (docs/adr/0002-canary-rollout-with-statefulset-partition.md).
	flag.DurationVar(&rec.Canary.Bake, "canary-bake", 2*time.Minute, "how long new config runs on the canary pod, once Ready, before it's promoted (at least twice the config's longest sink batch timeout)")
	flag.DurationVar(&rec.Canary.ReadyTimeout, "canary-ready-timeout", 3*time.Minute, "how long the canary pod may take to become Ready before the config is rolled back")
	flag.Float64Var(&rec.Canary.ErrorAllowance, "canary-error-allowance", 5, "the canary may log this many more errors than a stable pod during the bake...")
	flag.Float64Var(&rec.Canary.ErrorRatio, "canary-error-ratio", 0.001, "...or this share of the events its sinks took in, whichever is larger")
	metricsPort := flag.Int("aggregator-metrics-port", 9598, "port of the aggregator's Prometheus exporter, read to judge the canary")
	flag.StringVar(&rec.Namespace, "aggregator-namespace", "vector", "namespace of the Vector aggregator")
	flag.StringVar(&rec.StatefulSet, "aggregator-statefulset", "vector-aggregator", "the aggregator's StatefulSet")
	flag.StringVar(&rec.ConfigMap, "aggregator-configmap", "vector-aggregator-config", "the ConfigMap the aggregator loads its config from")
	flag.StringVar(&cold.Bucket, "cold-bucket", "", "S3 bucket for cold storage; empty disables cold routing")
	flag.StringVar(&cold.Endpoint, "cold-endpoint", "", "S3 endpoint, e.g. http://minio.storage.svc:9000 (empty for AWS)")
	flag.StringVar(&cold.Region, "cold-region", "us-east-1", "S3 region")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "address for /metrics")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "address for /healthz and /readyz")
	flag.BoolVar(&leaderElect, "leader-elect", false, "elect a leader, so only one replica reconciles at a time")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if cold.Bucket != "" {
		rec.Options.Cold = &cold
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	// Without Vector there is no validation gate, so don't run at all.
	version, err := exec.Command(vector.Bin, "--version").Output()
	if err != nil {
		log.Error(err, "the vector binary is needed to validate rendered config", "path", vector.Bin)
		os.Exit(1)
	}
	log.Info("validating rendered config with", "vector", strings.TrimSpace(string(version)))
	rec.Validator = &validate.Cached{Validator: vector, Size: 64}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error(err, "registering client-go types")
		os.Exit(1)
	}
	if err := spillwayv1alpha1.AddToScheme(scheme); err != nil {
		log.Error(err, "registering spillway types")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "spillway-operator.spillway.dev",
		// The operator may only read ConfigMaps, StatefulSets and pods in the
		// aggregator's namespace, so it caches only those.
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}:   {Namespaces: map[string]cache.Config{rec.Namespace: {}}},
			&appsv1.StatefulSet{}: {Namespaces: map[string]cache.Config{rec.Namespace: {}}},
			&corev1.Pod{}:         {Namespaces: map[string]cache.Config{rec.Namespace: {}}},
		}},
	})
	if err != nil {
		log.Error(err, "creating manager")
		os.Exit(1)
	}

	rec.Client = mgr.GetClient()
	rec.Health = controller.MetricsHealth{Client: mgr.GetClient(), Port: *metricsPort}
	if err := rec.SetupWithManager(mgr); err != nil {
		log.Error(err, "setting up the aggregator controller")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "adding health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "adding ready check")
		os.Exit(1)
	}

	log.Info("starting operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "running manager")
		os.Exit(1)
	}
}
