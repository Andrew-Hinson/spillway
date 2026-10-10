package controller

// These tests run the reconciler against a real API server (envtest), with
// the LogPipeline CRD installed. envtest runs no controllers, so settle()
// plays the StatefulSet controller's part (honouring the partition), a fake
// clock drives the canary's bake, and a fake health checker stands in for the
// aggregator pods' metrics. Run them with `make test`.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/render"
	"github.com/Andrew-Hinson/spillway/internal/validate"
)

var k8s client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("CI") != "" {
			fmt.Fprintln(os.Stderr, "KUBEBUILDER_ASSETS is not set; run the tests with `make test`")
			os.Exit(1)
		}
		fmt.Println("skipping controller tests: KUBEBUILDER_ASSETS is not set (run `make test`)")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "deploy", "operator", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting envtest:", err)
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, spillwayv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		fmt.Fprintln(os.Stderr, "creating client:", err)
		os.Exit(1)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stopping envtest:", err)
	}
	os.Exit(code)
}

// env is one test's aggregator: its own namespace, a two-replica StatefulSet,
// and a reconciler with a fake clock, validator and health checker.
type env struct {
	t      *testing.T
	ctx    context.Context
	r      *AggregatorReconciler
	v      *fakeVector
	health *fakeHealth
	clock  time.Time
}

// fakeVector stands in for `vector validate`: it rejects any config that
// contains all of the strings in one of its rules. (internal/validate tests
// the real binary.)
type fakeVector struct {
	rejectIfAll [][]string
	calls       int
}

func (f *fakeVector) Validate(_ context.Context, cfg []byte) error {
	f.calls++
	for _, rule := range f.rejectIfAll {
		all := true
		for _, s := range rule {
			all = all && strings.Contains(string(cfg), s)
		}
		if all {
			return &validate.Error{Output: fmt.Sprintf("rejected: config contains %q", rule)}
		}
	}
	return nil
}

// fakeHealth returns whatever the test has set for each pod.
type fakeHealth struct{ pods map[string]PodHealth }

func (f *fakeHealth) Check(_ context.Context, _, pod string) (PodHealth, error) {
	return f.pods[pod], nil
}

var canary = CanarySettings{Bake: 2 * time.Minute, ReadyTimeout: 3 * time.Minute, ErrorAllowance: 5, ErrorRatio: 0.001}

// newEnv creates the test's namespace and reconciler, and an aggregator
// StatefulSet unless withAggregator is false. selects controls whether the
// aggregator's container selects its config file from the suffix annotation.
func newEnv(t *testing.T, withAggregator, selects bool) *env {
	t.Helper()
	// A namespace per test, named after it: lowercase, [a-z0-9-] only, and
	// short enough, with a hash of the full name to keep it unique.
	ns := strings.Trim(regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(t.Name()), "-"), "-")
	if len(ns) > 50 {
		sum := sha256.Sum256([]byte(t.Name()))
		ns = strings.Trim(ns[:41], "-") + "-" + hex.EncodeToString(sum[:4])
	}
	ctx := context.Background()
	must(t, k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	opts := render.DefaultOptions()
	opts.Cold = nil // as the operator runs until cold storage is configured
	e := &env{t: t, ctx: ctx, v: &fakeVector{}, health: &fakeHealth{pods: map[string]PodHealth{}},
		clock: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	e.r = &AggregatorReconciler{
		Client: k8s, Options: opts, Validator: e.v, Health: e.health, Canary: canary,
		Now:       func() time.Time { return e.clock },
		Namespace: ns, StatefulSet: "vector-aggregator", ConfigMap: "vector-aggregator-config",
	}
	// The bootstrap config `make aggregator` seeds.
	must(t, k8s.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: e.r.ConfigMap},
		Data:       map[string]string{BootstrapKey: "# bootstrap\n"},
	}))
	if withAggregator {
		labels := map[string]string{"app": "vector-aggregator"}
		c := corev1.Container{Name: "vector", Image: "vector", Args: []string{"--config-dir", "/etc/vector/"}}
		if selects {
			c.Args = []string{"--config", "/etc/vector/vector$(" + SuffixEnv + ").yaml"}
			c.Env = []corev1.EnvVar{{Name: SuffixEnv, ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + SuffixAnnotation + "']"}}}}
		}
		replicas := int32(2)
		must(t, k8s.Create(ctx, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: e.r.StatefulSet},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{c}},
				},
			},
		}))
		e.settle()
	}
	return e
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// pipeline creates a LogPipeline reading pod logs from one namespace.
func (e *env) pipeline(name, team, namespace string, cold bool) {
	e.t.Helper()
	lp := &spillwayv1alpha1.LogPipeline{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.r.Namespace, Name: name},
		Spec: spillwayv1alpha1.LogPipelineSpec{
			Team: team,
			Sources: []spillwayv1alpha1.Source{{
				Name:       "pods",
				Kubernetes: &spillwayv1alpha1.KubernetesSource{Namespaces: []string{namespace}},
			}},
		},
	}
	if cold {
		yes := true
		lp.Spec.Routing = &spillwayv1alpha1.Routing{Cold: &yes}
	}
	must(e.t, k8s.Create(e.ctx, lp))
	// The reconciler sees pipelines in every namespace, so remove them before
	// the next test runs.
	e.t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), lp)) })
}

func (e *env) deletePipeline(name string) {
	e.t.Helper()
	must(e.t, k8s.Delete(e.ctx, &spillwayv1alpha1.LogPipeline{ObjectMeta: metav1.ObjectMeta{Namespace: e.r.Namespace, Name: name}}))
}

// setBudget changes a pipeline's spec, so its config (and hash) changes.
func (e *env) setBudget(name string, perSec int32) {
	e.t.Helper()
	var lp spillwayv1alpha1.LogPipeline
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: name}, &lp))
	lp.Spec.Budget = &spillwayv1alpha1.Budget{MaxEventsPerSec: perSec}
	must(e.t, k8s.Update(e.ctx, &lp))
}

func (e *env) reconcile() ctrl.Result {
	e.t.Helper()
	res, err := e.r.Reconcile(e.ctx, ctrl.Request{})
	must(e.t, err)
	return res
}

func (e *env) advance(d time.Duration) { e.clock = e.clock.Add(d) }

func (e *env) sts() *appsv1.StatefulSet {
	e.t.Helper()
	var sts appsv1.StatefulSet
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.StatefulSet}, &sts))
	return &sts
}

// settle does what the StatefulSet controller would once it's done: pods at
// or above the partition run the current template, and every pod is Ready.
func (e *env) settle() {
	e.t.Helper()
	sts := e.sts()
	replicas, partition := replicasOf(sts), partitionOf(sts)
	update := fmt.Sprintf("rev-%d", sts.Generation)
	current := update
	if partition > 0 {
		current = "rev-stable"
	}
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: sts.Generation, Replicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas,
		UpdatedReplicas: replicas - partition, CurrentRevision: current, UpdateRevision: update,
	}
	must(e.t, k8s.Status().Update(e.ctx, sts))
}

// running returns the config hash the pod template points at ("" for bootstrap).
func (e *env) running() string {
	return strings.TrimPrefix(e.sts().Spec.Template.Annotations[SuffixAnnotation], "-")
}

func (e *env) configMap() map[string]string {
	e.t.Helper()
	var cm corev1.ConfigMap
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.ConfigMap}, &cm))
	return cm.Data
}

// config returns the config file the pod template points at.
func (e *env) config() string {
	e.t.Helper()
	cfg, ok := e.configMap()[configKey(e.running())]
	if !ok {
		e.t.Fatalf("the ConfigMap has no %s for the pod template", configKey(e.running()))
	}
	return cfg
}

// promote takes the desired config through a healthy canary to every pod.
func (e *env) promote() {
	e.t.Helper()
	e.reconcile() // canary starts
	e.settle()
	e.reconcile() // baseline taken
	e.advance(canary.Bake + time.Second)
	e.reconcile() // promoted
	e.settle()
	e.reconcile() // recorded as stable
	if e.sts().Annotations[annStable] != e.running() || partitionOf(e.sts()) != 0 {
		e.t.Fatalf("not promoted: stable=%q running=%q partition=%d", e.sts().Annotations[annStable], e.running(), partitionOf(e.sts()))
	}
}

// expectReady checks a pipeline's Ready condition.
func (e *env) expectReady(name string, status metav1.ConditionStatus, reason, messagePart string) {
	e.t.Helper()
	var lp spillwayv1alpha1.LogPipeline
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: name}, &lp))
	c := meta.FindStatusCondition(lp.Status.Conditions, spillwayv1alpha1.ConditionReady)
	if c == nil {
		e.t.Fatalf("%s: no Ready condition", name)
	}
	if c.Status != status || c.Reason != reason || !strings.Contains(c.Message, messagePart) {
		e.t.Fatalf("%s: Ready=%s reason=%s message=%q; want Ready=%s reason=%s message containing %q",
			name, c.Status, c.Reason, c.Message, status, reason, messagePart)
	}
	if lp.Status.ObservedGeneration != lp.Generation {
		e.t.Errorf("%s: observedGeneration %d, want %d", name, lp.Status.ObservedGeneration, lp.Generation)
	}
}

func TestNewConfigCanariesThenPromotes(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)

	e.reconcile()
	hash := e.running()
	if hash == "" || partitionOf(e.sts()) != 1 || e.sts().Annotations[annCanary] != hash {
		t.Fatalf("canary not started: running=%q partition=%d", hash, partitionOf(e.sts()))
	}
	if !strings.Contains(e.config(), "search_hot:") {
		t.Fatal("the canary's config file lacks the pipeline")
	}
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "starting on canary pod vector-aggregator-1")

	e.settle()
	e.reconcile()
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "running on vector-aggregator-1 (1 of 2 aggregators)")

	// Not promoted before the bake is over, however healthy.
	e.advance(canary.Bake - time.Second)
	e.reconcile()
	if partitionOf(e.sts()) != 1 {
		t.Fatal("promoted before the bake finished")
	}

	e.advance(2 * time.Second)
	e.reconcile()
	if partitionOf(e.sts()) != 0 || e.sts().Annotations[annCanary] != "" {
		t.Fatalf("not promoted after a healthy bake: partition=%d", partitionOf(e.sts()))
	}
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "passed its canary")

	e.settle()
	e.reconcile()
	e.expectReady("search", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "running config")
	if e.sts().Annotations[annStable] != hash {
		t.Errorf("stable-config = %q, want %q", e.sts().Annotations[annStable], hash)
	}

	// Nothing changed: reconciling again starts no new canary.
	e.reconcile()
	if e.running() != hash || e.sts().Annotations[annCanary] != "" {
		t.Error("an unchanged config started another rollout")
	}
}

func TestErroringCanaryIsRolledBackAndQuarantined(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()
	stable := e.running()

	e.pipeline("beta", "beta", "beta", false)
	e.reconcile()
	e.settle()
	e.reconcile() // baseline
	e.expectReady("search", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")

	// The canary errors well beyond the stable pod: rolled back mid-bake.
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50}
	e.health.pods["vector-aggregator-0"] = PodHealth{Errors: 2}
	e.advance(30 * time.Second)
	e.reconcile()
	if e.running() != stable || partitionOf(e.sts()) != 0 {
		t.Fatalf("not rolled back: running=%q, want %q", e.running(), stable)
	}
	e.expectReady("beta", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "logged 50 errors in 30s against 2 on stable pod vector-aggregator-0")
	e.expectReady("search", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")

	// beta's generation is quarantined, so it isn't tried again on its own...
	e.settle()
	e.advance(time.Hour)
	e.reconcile()
	if e.running() != stable || e.sts().Annotations[annCanary] != "" {
		t.Fatal("a quarantined change was retried without a spec change")
	}
	e.expectReady("beta", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "Change the spec to try again")

	// ...but a spec change is a new generation, which gets its own canary.
	e.setBudget("beta", 10)
	e.reconcile()
	if r := e.running(); r == stable || e.sts().Annotations[annCanary] != r || !strings.Contains(e.config(), "beta_budget") {
		t.Fatalf("no new canary for beta after a spec change: running=%q", r)
	}
	if len(quarantineOf(e.sts())) != 0 {
		t.Error("the stale quarantine entry wasn't dropped after beta changed")
	}
}

// One team's failed change must not ride along with, and sink, everyone
// else's later changes.
func TestFailedChangeDoesNotBlockOtherTeams(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()

	e.pipeline("beta", "beta", "beta", false)
	e.reconcile()
	e.settle()
	e.reconcile()
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50}
	e.advance(10 * time.Second)
	e.reconcile()                                                // beta rolled back and quarantined
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50} // counters stay where they are

	e.setBudget("search", 10)
	e.reconcile()
	if strings.Contains(e.config(), "beta") {
		t.Fatal("the quarantined pipeline rode along with another team's change")
	}
	if !strings.Contains(e.config(), "search_budget") {
		t.Fatal("the other team's change isn't in the canary")
	}
	e.promote()
	e.expectReady("search", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
	e.expectReady("beta", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "")
}

// A pipeline whose new spec fails its canary keeps running its last good spec.
func TestChangedPipelineKeepsItsLastGoodSpec(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()
	stable := e.running()
	var lp spillwayv1alpha1.LogPipeline
	must(t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: "search"}, &lp))
	if !strings.Contains(lp.Annotations[LastGoodSpecAnnotation], `"generation":1`) {
		t.Fatalf("last good spec not recorded at promotion: %q", lp.Annotations[LastGoodSpecAnnotation])
	}

	e.setBudget("search", 10)
	e.reconcile()
	e.settle()
	e.reconcile()
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50}
	e.advance(10 * time.Second)
	e.reconcile() // rolled back

	e.settle()
	e.reconcile()
	if e.running() != stable || strings.Contains(e.config(), "search_budget") || !strings.Contains(e.config(), "search_hot") {
		t.Fatalf("search should keep running its last good spec: running=%q, want %q", e.running(), stable)
	}
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "generation 1 keeps running")
}

// A canary that fails with no pipeline change to blame (here, the operator's
// first config on a fresh aggregator) isn't retried in a loop.
func TestFailedCanaryWithNoPipelineChangeIsNotRetried(t *testing.T) {
	e := newEnv(t, true, true)
	e.reconcile() // the first rendered config goes to the canary
	first := e.running()
	e.advance(canary.ReadyTimeout + time.Second)
	e.reconcile() // never Ready: rolled back to the bootstrap config
	if e.running() != "" || e.sts().Annotations[annRejected] != first {
		t.Fatalf("running=%q rejected=%q; want bootstrap and %q", e.running(), e.sts().Annotations[annRejected], first)
	}
	e.settle()
	e.advance(time.Hour)
	e.reconcile()
	if e.running() != "" || e.sts().Annotations[annCanary] != "" {
		t.Fatal("a rejected config with nothing to quarantine was retried")
	}
}

func TestRestartingCanaryIsRolledBack(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	e.settle()
	e.reconcile() // baseline
	e.health.pods["vector-aggregator-1"] = PodHealth{Restarts: 1}
	e.advance(10 * time.Second)
	e.reconcile()
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "restarted 1 times")
}

func TestErrorsWithinAllowanceArePromoted(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	e.settle()
	e.reconcile() // baseline
	// Both pods see errors (a shared upstream hiccup): only the excess counts.
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 40}
	e.health.pods["vector-aggregator-0"] = PodHealth{Errors: 37}
	e.advance(canary.Bake + time.Second)
	e.reconcile()
	if partitionOf(e.sts()) != 0 || len(quarantineOf(e.sts())) != 0 {
		t.Fatal("a canary within the error allowance was not promoted")
	}
}

func TestCanaryWithAStalledSinkIsRolledBack(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	e.settle()
	e.reconcile() // baseline: nothing received yet
	// The canary's new sink takes events in but never delivers, without
	// logging errors (it keeps retrying); the stable pod doesn't have it.
	e.health.pods["vector-aggregator-1"] = PodHealth{Sinks: map[string]SinkCounts{"hot_loki": {Received: 400}}}
	e.advance(time.Minute)
	e.reconcile()
	if e.sts().Annotations[annCanary] == "" {
		t.Fatal("judged a stall before the bake finished; sinks may batch for a minute")
	}
	e.advance(canary.Bake)
	e.reconcile()
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "took in events for hot_loki but delivered none")
}

func TestNewSpecMidCanaryReplacesTheCanary(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	first := e.running()
	e.settle()
	e.reconcile() // baseline for the first canary

	e.setBudget("search", 10)
	e.reconcile()
	second := e.running()
	if second == first || e.sts().Annotations[annCanary] != second || e.sts().Annotations[annCanaryBaseline] != "" {
		t.Fatalf("canary not replaced: running=%q canary=%q", second, e.sts().Annotations[annCanary])
	}
	if partitionOf(e.sts()) != 1 {
		t.Error("the replacement canary isn't confined to one pod")
	}
}

func TestRevertMidCanaryReturnsToStableWithoutACanary(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()
	stable := e.running()

	e.pipeline("beta", "beta", "beta", false)
	e.reconcile()
	e.deletePipeline("beta") // reverted before the canary finished
	e.reconcile()
	if e.running() != stable || partitionOf(e.sts()) != 0 || e.sts().Annotations[annCanary] != "" {
		t.Fatalf("not back to stable: running=%q partition=%d", e.running(), partitionOf(e.sts()))
	}
}

func TestConfigMapKeepsOnlyLiveRevisions(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()
	first := e.running()
	e.setBudget("search", 10)
	e.promote()
	e.reconcile() // prune with the new stable recorded
	cm := e.configMap()
	if _, ok := cm[configKey(first)]; ok {
		t.Error("a superseded revision was not pruned")
	}
	if _, ok := cm[BootstrapKey]; !ok {
		t.Error("the bootstrap config was pruned")
	}
	if _, ok := cm[configKey(e.running())]; !ok || len(cm) != 2 {
		t.Errorf("ConfigMap keys = %v, want the bootstrap and the running revision", keys(cm))
	}
}

func keys(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestConflictingPipelineIsInvalidAndNeverRendered(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("a-first", "search", "search", false)
	e.promote()
	before := e.running()

	// Same team, created later (or, within the same second, sorted after).
	e.pipeline("b-second", "search", "other", false)
	e.reconcile()
	e.expectReady("b-second", metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, `team "search" is claimed by both`)
	e.expectReady("a-first", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
	if e.running() != before {
		t.Error("an invalid pipeline changed the running config")
	}

	// Once the first is deleted, the second no longer conflicts and is rolled out.
	e.deletePipeline("a-first")
	e.promote()
	e.expectReady("b-second", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
	if !strings.Contains(e.config(), `"other"`) {
		t.Error("the second pipeline's namespace is missing from the config after the conflict cleared")
	}
}

func TestColdRoutingWithoutStorageIsInvalid(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("archive", "archive", "archive", true)
	e.reconcile()
	e.expectReady("archive", metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, "no cold storage is configured")
	if strings.Contains(e.config(), "cold_s3") {
		t.Error("cold sink rendered without cold storage")
	}
}

func TestOverwrittenConfigIsRestored(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()
	want := e.config()

	var cm corev1.ConfigMap
	must(t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.ConfigMap}, &cm))
	cm.Data[configKey(e.running())] = "# edited by hand\n"
	must(t, k8s.Update(e.ctx, &cm))

	e.reconcile()
	if got := e.config(); got != want {
		t.Errorf("config not restored after a manual edit:\n%s", got)
	}
}

func TestMissingAggregatorIsReported(t *testing.T) {
	e := newEnv(t, false, true)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonAggregatorNotFound, "not found")
}

func TestAggregatorThatCantSelectConfigIsReported(t *testing.T) {
	e := newEnv(t, true, false)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonAggregatorMisconfigured, "doesn't select its config file")
	if e.running() != "" || partitionOf(e.sts()) != 0 {
		t.Error("rolled out to an aggregator that can't select its config")
	}
}

func TestPipelineVectorRejectsIsInvalidAndOthersRollOut(t *testing.T) {
	e := newEnv(t, true, true)
	e.v.rejectIfAll = [][]string{{"poison_hot"}}
	e.pipeline("good", "good", "good", false)
	e.pipeline("poison", "poison", "poison", false)

	e.reconcile()
	e.expectReady("poison", metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, `rendered config rejected by vector validate: rejected: config contains ["poison_hot"]`)
	if cfg := e.config(); strings.Contains(cfg, "poison") || !strings.Contains(cfg, "good_hot") {
		t.Error("the config should hold the valid pipeline and not the rejected one")
	}
	e.promote()
	e.expectReady("good", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
}

func TestConfigThatFailsOnlyCombinedIsNotApplied(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("alpha", "alpha", "alpha", false)
	e.promote()
	before := e.running()

	// Each pipeline validates alone, but not together.
	e.v.rejectIfAll = [][]string{{"alpha_hot", "beta_hot"}}
	e.pipeline("beta", "beta", "beta", false)
	e.reconcile()
	if e.running() != before || partitionOf(e.sts()) != 0 {
		t.Fatal("a config that failed validation was rolled out")
	}
	for _, name := range []string{"alpha", "beta"} {
		e.expectReady(name, metav1.ConditionFalse, spillwayv1alpha1.ReasonValidationFailed, "keeps its last valid config")
	}

	// Once the conflict is gone, the new config validates and gets a canary.
	e.deletePipeline("alpha")
	e.reconcile()
	e.expectReady("beta", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "canary")
}

func TestUnchangedConfigIsValidatedOnce(t *testing.T) {
	e := newEnv(t, true, true)
	e.r.Validator = &validate.Cached{Validator: e.v, Size: 8}
	e.pipeline("search", "search", "search", false)
	for range 5 {
		e.reconcile()
	}
	if e.v.calls != 1 {
		t.Errorf("validated an unchanged config %d times, want 1", e.v.calls)
	}
}

// withColdStorage configures cold storage, as --cold-bucket does.
func (e *env) withColdStorage() {
	e.r.Options.Cold = render.DefaultOptions().Cold
}

// failCanary takes the desired config through a canary that errors.
func (e *env) failCanary() {
	e.t.Helper()
	e.reconcile() // canary starts
	e.settle()
	e.health.pods["vector-aggregator-1"] = PodHealth{}
	e.reconcile() // baseline
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50}
	e.advance(10 * time.Second)
	e.reconcile() // rolled back
	e.health.pods["vector-aggregator-1"] = PodHealth{}
	e.settle()
}

func TestBakeIsAtLeastTwiceTheLongestSinkBatch(t *testing.T) {
	e := newEnv(t, true, true)
	e.withColdStorage() // cold_s3 batches for 60s
	e.r.Canary.Bake = 30 * time.Second
	e.pipeline("archive", "archive", "archive", true)
	e.reconcile()
	e.settle()
	e.reconcile() // baseline
	e.expectReady("archive", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "promoting at 2026-10-09T12:02:00Z")
	e.advance(time.Minute)
	e.reconcile()
	if partitionOf(e.sts()) != 1 {
		t.Fatal("promoted after the configured 30s, before a 60s batch could be sent twice")
	}
	e.advance(time.Minute + time.Second)
	e.reconcile()
	if partitionOf(e.sts()) != 0 {
		t.Fatal("not promoted after twice the longest batch timeout")
	}
}

func TestErrorAllowanceScalesWithTraffic(t *testing.T) {
	for _, tc := range []struct {
		events   float64
		promoted bool
	}{{100000, true}, {1000, false}} {
		t.Run(fmt.Sprintf("%.0f events", tc.events), func(t *testing.T) {
			e := newEnv(t, true, true)
			e.pipeline("search", "search", "search", false)
			e.reconcile()
			e.settle()
			e.reconcile() // baseline
			// 50 errors: within 0.1% of 100k events, beyond the floor of 5 for 1k.
			e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50, Sinks: map[string]SinkCounts{"hot_loki": {Received: tc.events, Sent: tc.events}}}
			e.advance(canary.Bake + time.Second)
			e.reconcile()
			if promoted := partitionOf(e.sts()) == 0 && len(quarantineOf(e.sts())) == 0; promoted != tc.promoted {
				t.Fatalf("promoted = %v, want %v", promoted, tc.promoted)
			}
			if !tc.promoted {
				e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "(allowed: 5 more, for 1000 events)")
			}
		})
	}
}

// Changes that fail together are retried one at a time; only the one that
// fails alone stays quarantined.
func TestChangesThatFailTogetherAreRetriedAlone(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.promote()
	e.pipeline("beta", "beta", "beta", false)
	e.pipeline("gamma", "gamma", "gamma", false)
	e.failCanary()

	q := quarantineOf(e.sts())
	if len(q) != 2 {
		t.Fatalf("quarantine = %v, want both changes", q)
	}
	for _, entry := range q {
		if !entry.Retry {
			t.Fatal("changes that failed together were quarantined without a retry")
		}
	}

	// The next canary holds exactly one of them.
	e.reconcile()
	cfg := e.config()
	first, second := "beta", "gamma"
	if strings.Contains(cfg, "gamma_hot") {
		first, second = "gamma", "beta"
	}
	if !strings.Contains(cfg, first+"_hot") || strings.Contains(cfg, second+"_hot") {
		t.Fatal("the retry canary should hold exactly one of the changes")
	}
	e.expectReady(second, metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "retried in a canary of its own (1 ahead of it)")

	// The first passes alone and is released...
	e.settle()
	e.reconcile() // baseline
	e.advance(canary.Bake + time.Second)
	e.reconcile()
	e.settle()
	e.reconcile()
	e.expectReady(first, metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")

	// ...then the second gets its own canary, fails alone, and stays quarantined.
	e.failCanary()
	e.reconcile()
	e.expectReady(second, metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed, "Change the spec to try again")
	e.expectReady(first, metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
	if strings.Contains(e.config(), second+"_hot") {
		t.Error("the change that failed alone is still in the config")
	}
}

func TestRollbackReportsOrphanedBuffers(t *testing.T) {
	e := newEnv(t, true, true)
	e.withColdStorage()
	e.pipeline("search", "search", "search", false)
	e.promote()
	e.pipeline("archive", "archive", "archive", true) // adds the disk-buffered cold_s3 sink
	e.reconcile()
	e.settle()
	e.reconcile()
	e.health.pods["vector-aggregator-1"] = PodHealth{Errors: 50}
	e.advance(10 * time.Second)
	e.reconcile()
	e.expectReady("archive", metav1.ConditionFalse, spillwayv1alpha1.ReasonCanaryFailed,
		"events cold_s3 buffered on the canary pod weren't delivered (issue #43)")
	e.expectReady("search", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
}

// Each aggregator enforces its share of a team's budget, so the operator
// renders with the StatefulSet's replica count, and scaling it rolls out
// config with the new share.
func TestBudgetIsSharedAcrossAggregators(t *testing.T) {
	e := newEnv(t, true, true)
	e.pipeline("search", "search", "search", false)
	e.setBudget("search", 30)
	e.promote()
	if !strings.Contains(e.config(), "threshold: 150\n") { // 15/s per pod, over a 10 s window
		t.Fatalf("2 aggregators: want each to allow 15/s of a 30/s budget:\n%s", e.config())
	}
	before := e.running()

	sts := e.sts()
	three := int32(3)
	sts.Spec.Replicas = &three
	must(t, k8s.Update(e.ctx, sts))
	e.settle()
	e.reconcile()
	if e.running() == before || !strings.Contains(e.config(), "threshold: 100\n") {
		t.Fatalf("scaled to 3: want a new config allowing 10/s per pod:\n%s", e.config())
	}
}
