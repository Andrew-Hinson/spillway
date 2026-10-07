package controller

// These tests run the reconciler against a real API server (envtest), with
// the LogPipeline CRD installed. envtest runs no controllers, so the tests
// play the StatefulSet controller's part by marking rollouts complete. Run
// them with `make test`.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// env is one test's aggregator: its own namespace, StatefulSet and reconciler.
type env struct {
	t   *testing.T
	ctx context.Context
	r   *AggregatorReconciler
	v   *fakeVector
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

func newEnv(t *testing.T, createStatefulSet bool) *env {
	t.Helper()
	ns := strings.ToLower(strings.ReplaceAll(t.Name(), "_", "-"))
	if len(ns) > 50 {
		ns = ns[:50]
	}
	ctx := context.Background()
	must(t, k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	opts := render.DefaultOptions()
	opts.Cold = nil // as the operator runs until cold storage is configured
	v := &fakeVector{}
	e := &env{t: t, ctx: ctx, v: v, r: &AggregatorReconciler{
		Client: k8s, Options: opts, Validator: v,
		Namespace: ns, StatefulSet: "vector-aggregator", ConfigMap: "vector-aggregator-config",
	}}
	if createStatefulSet {
		labels := map[string]string{"app": "vector-aggregator"}
		must(t, k8s.Create(ctx, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: e.r.StatefulSet},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "vector", Image: "vector"}}},
				},
			},
		}))
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

func (e *env) reconcile() {
	e.t.Helper()
	_, err := e.r.Reconcile(e.ctx, ctrl.Request{})
	must(e.t, err)
}

// finishRollout does what the StatefulSet controller would once every pod
// runs the current template.
func (e *env) finishRollout() {
	e.t.Helper()
	var sts appsv1.StatefulSet
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.StatefulSet}, &sts))
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: sts.Generation,
		Replicas:           1, ReadyReplicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1,
		CurrentRevision: fmt.Sprintf("rev-%d", sts.Generation), UpdateRevision: fmt.Sprintf("rev-%d", sts.Generation),
	}
	must(e.t, k8s.Status().Update(e.ctx, &sts))
}

func (e *env) config() string {
	e.t.Helper()
	var cm corev1.ConfigMap
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.ConfigMap}, &cm))
	return cm.Data[ConfigKey]
}

func (e *env) hash() string {
	e.t.Helper()
	var sts appsv1.StatefulSet
	must(e.t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.StatefulSet}, &sts))
	return sts.Spec.Template.Annotations[HashAnnotation]
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

func TestPipelineIsRenderedRolledOutAndReady(t *testing.T) {
	e := newEnv(t, true)
	e.pipeline("search", "search", "search", false)

	e.reconcile()
	if cfg := e.config(); !strings.Contains(cfg, "search_hot:") {
		t.Fatalf("rendered config has no search_hot component:\n%s", cfg)
	}
	first := e.hash()
	if first == "" {
		t.Fatal("StatefulSet pod template has no config hash")
	}
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "restarting")

	e.finishRollout()
	e.reconcile()
	e.expectReady("search", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "running config")

	// Nothing changed: reconciling again must not roll the aggregator.
	e.reconcile()
	if got := e.hash(); got != first {
		t.Errorf("hash changed from %s to %s with no spec change", first, got)
	}
}

func TestConflictingPipelineIsInvalidAndNeverRendered(t *testing.T) {
	e := newEnv(t, true)
	e.pipeline("a-first", "search", "search", false)
	e.reconcile()
	e.finishRollout()
	e.reconcile()
	before := e.config()

	// Same team, created later (or, within the same second, sorted after).
	e.pipeline("b-second", "search", "other", false)
	e.reconcile()
	e.expectReady("b-second", metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, `team "search" is claimed by both`)
	e.expectReady("a-first", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
	if after := e.config(); after != before {
		t.Error("an invalid pipeline changed the rendered config")
	}
	if strings.Contains(e.config(), `"other"`) {
		t.Error("the invalid pipeline's namespace reached the config")
	}

	// Once the first is deleted, the second no longer conflicts and is rolled out.
	must(t, k8s.Delete(e.ctx, &spillwayv1alpha1.LogPipeline{ObjectMeta: metav1.ObjectMeta{Namespace: e.r.Namespace, Name: "a-first"}}))
	e.reconcile()
	e.expectReady("b-second", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "")
	e.finishRollout()
	e.reconcile()
	e.expectReady("b-second", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
	if cfg := e.config(); !strings.Contains(cfg, `"other"`) {
		t.Error("the second pipeline's namespace is missing from the config after the conflict cleared")
	}
}

func TestColdRoutingWithoutStorageIsInvalid(t *testing.T) {
	e := newEnv(t, true)
	e.pipeline("archive", "archive", "archive", true)
	e.reconcile()
	e.expectReady("archive", metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, "no cold storage is configured")
	if strings.Contains(e.config(), "cold_s3") {
		t.Error("cold sink rendered without cold storage")
	}
}

func TestOverwrittenConfigIsRestored(t *testing.T) {
	e := newEnv(t, true)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	want := e.config()

	var cm corev1.ConfigMap
	must(t, k8s.Get(e.ctx, types.NamespacedName{Namespace: e.r.Namespace, Name: e.r.ConfigMap}, &cm))
	cm.Data[ConfigKey] = "# edited by hand\n"
	must(t, k8s.Update(e.ctx, &cm))

	e.reconcile()
	if got := e.config(); got != want {
		t.Errorf("config not restored after a manual edit:\n%s", got)
	}
}

func TestMissingAggregatorIsReported(t *testing.T) {
	e := newEnv(t, false)
	e.pipeline("search", "search", "search", false)
	e.reconcile()
	e.expectReady("search", metav1.ConditionFalse, spillwayv1alpha1.ReasonAggregatorNotFound, "not found")
}

func TestPipelineVectorRejectsIsInvalidAndOthersRollOut(t *testing.T) {
	e := newEnv(t, true)
	e.v.rejectIfAll = [][]string{{"poison_hot"}}
	e.pipeline("good", "good", "good", false)
	e.pipeline("poison", "poison", "poison", false)

	e.reconcile()
	e.expectReady("poison", metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, `rendered config rejected by vector validate: rejected: config contains ["poison_hot"]`)
	e.expectReady("good", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "")
	cfg := e.config()
	if strings.Contains(cfg, "poison") {
		t.Error("a pipeline Vector rejects reached the aggregator config")
	}
	if !strings.Contains(cfg, "good_hot") {
		t.Error("the valid pipeline is missing from the config")
	}
	e.finishRollout()
	e.reconcile()
	e.expectReady("good", metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut, "")
}

func TestConfigThatFailsOnlyCombinedIsNotApplied(t *testing.T) {
	e := newEnv(t, true)
	e.pipeline("alpha", "alpha", "alpha", false)
	e.reconcile()
	e.finishRollout()
	e.reconcile()
	before, hash := e.config(), e.hash()

	// Each pipeline validates alone, but not together.
	e.v.rejectIfAll = [][]string{{"alpha_hot", "beta_hot"}}
	e.pipeline("beta", "beta", "beta", false)
	e.reconcile()

	if e.config() != before || e.hash() != hash {
		t.Fatal("a config that failed validation was applied")
	}
	for _, name := range []string{"alpha", "beta"} {
		e.expectReady(name, metav1.ConditionFalse, spillwayv1alpha1.ReasonValidationFailed, "keeps its last valid config")
	}

	// Once the conflict is gone, the new config validates and rolls out.
	must(t, k8s.Delete(e.ctx, &spillwayv1alpha1.LogPipeline{ObjectMeta: metav1.ObjectMeta{Namespace: e.r.Namespace, Name: "alpha"}}))
	e.reconcile()
	e.expectReady("beta", metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut, "")
}

func TestUnchangedConfigIsValidatedOnce(t *testing.T) {
	e := newEnv(t, true)
	e.r.Validator = &validate.Cached{Validator: e.v, Size: 8}
	e.pipeline("search", "search", "search", false)
	for range 5 {
		e.reconcile()
	}
	if e.v.calls != 1 {
		t.Errorf("validated an unchanged config %d times, want 1", e.v.calls)
	}
}
