// Package controller reconciles LogPipelines into running Vector config.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/render"
	"github.com/Andrew-Hinson/spillway/internal/validate"
)

// AggregatorReconciler renders every LogPipeline into one aggregator config,
// applies it, rolls the aggregator, and reports each pipeline's status.
//
// There is one config for all teams, so every event maps to a single request
// and reconciles run one at a time.
type AggregatorReconciler struct {
	client.Client
	Options render.Options
	// Validator checks rendered config before it's applied.
	Validator validate.Validator
	// Health reads aggregator pods' health for the canary; Canary tunes it.
	Health HealthChecker
	Canary CanarySettings
	// Now is the clock (time.Now if nil); tests replace it.
	Now func() time.Time

	// The aggregator: its namespace, StatefulSet and config ConfigMap.
	Namespace   string
	StatefulSet string
	ConfigMap   string
}

// +kubebuilder:rbac:groups=spillway.dev,resources=logpipelines,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=spillway.dev,resources=logpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace=vector,resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,namespace=vector,resources=statefulsets,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",namespace=vector,resources=pods,verbs=get;list;watch

// Reconcile renders, applies and rolls out the aggregator config.
func (r *AggregatorReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var list spillwayv1alpha1.LogPipelineList
	if err := r.List(ctx, &list); err != nil {
		return ctrl.Result{}, err
	}
	accepted, invalid := r.partition(list.Items)

	var sts appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.StatefulSet}, &sts)
	if apierrors.IsNotFound(err) {
		r.report(ctx, list.Items, invalid, nil, rolloutState{phase: phaseRolling},
			spillwayv1alpha1.ReasonAggregatorNotFound, fmt.Sprintf("aggregator StatefulSet %s/%s not found", r.Namespace, r.StatefulSet))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if !selectsConfig(&sts) {
		r.report(ctx, list.Items, invalid, nil, rolloutState{phase: phaseRolling}, spillwayv1alpha1.ReasonAggregatorMisconfigured,
			fmt.Sprintf("aggregator StatefulSet %s/%s doesn't select its config file with $(%s) from the %s annotation (see vector/aggregator/values.yaml)",
				r.Namespace, r.StatefulSet, SuffixEnv, SuffixAnnotation))
		return ctrl.Result{}, nil
	}
	accepted, failed, err := r.applyQuarantine(ctx, &sts, list.Items, accepted)
	if err != nil {
		return ctrl.Result{}, err
	}

	cfg, accepted, err := r.validated(ctx, accepted, invalid)
	var rejected *validate.Error
	if errors.As(err, &rejected) {
		// Not even the pipelines that pass alone combine into a config Vector
		// accepts. Apply nothing: the aggregator keeps its last valid config.
		logger.Info("rendered config failed validation; keeping the current config", "output", rejected.Output)
		r.report(ctx, list.Items, invalid, failed, rolloutState{}, spillwayv1alpha1.ReasonValidationFailed,
			"the combined config failed validation, so the aggregator keeps its last valid config: "+rejected.Error())
		return ctrl.Result{}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}
	sum := sha256.Sum256(cfg)
	hash := hex.EncodeToString(sum[:])[:16]

	if err := r.applyConfigMap(ctx, &sts, hash, cfg); err != nil {
		return ctrl.Result{}, err
	}
	st, res, err := r.rollout(ctx, &sts, hash, accepted)
	if err != nil {
		return ctrl.Result{}, err
	}
	logger.V(1).Info("rollout", "hash", hash, "phase", st.phase, "message", st.message)
	r.report(ctx, list.Items, invalid, failed, st, "", st.message)
	return res, nil
}

// applyQuarantine leaves quarantined pipeline generations out of accepted. A
// quarantined pipeline with a last good spec keeps running that spec; one
// without (it never rolled out) is left out entirely. It returns why each
// quarantined pipeline was held back, and drops quarantine entries whose
// pipeline has since changed or gone.
func (r *AggregatorReconciler) applyQuarantine(ctx context.Context, sts *appsv1.StatefulSet,
	all, accepted []spillwayv1alpha1.LogPipeline) ([]spillwayv1alpha1.LogPipeline, map[types.UID]string, error) {
	q := quarantineOf(sts)
	if len(q) == 0 {
		return accepted, nil, nil
	}
	current := map[string]int64{}
	for _, p := range all {
		current[string(p.UID)] = p.Generation
	}
	stale := false
	for uid, e := range q {
		if current[uid] != e.Generation {
			delete(q, uid)
			stale = true
		}
	}
	if stale {
		raw, _ := json.Marshal(q)
		v := string(raw)
		patch := client.MergeFrom(sts.DeepCopy())
		setAnnotations(&sts.Annotations, map[string]*string{annQuarantine: &v})
		if err := r.Patch(ctx, sts, patch); err != nil {
			return nil, nil, err
		}
	}

	failed := map[types.UID]string{}
	kept := accepted[:0:0]
	for _, p := range accepted {
		e, held := q[string(p.UID)]
		if !held {
			kept = append(kept, p)
			continue
		}
		msg := fmt.Sprintf("generation %d %s", e.Generation, e.Reason)
		if last, ok := lastGoodSpec(&p); ok {
			p.Spec, p.Generation = last.Spec, last.Generation
			kept = append(kept, p)
			msg += fmt.Sprintf("; generation %d keeps running", last.Generation)
		}
		failed[p.UID] = msg + ". Change the spec to try again"
	}
	return kept, failed, nil
}

type goodSpec struct {
	Generation int64                            `json:"generation"`
	Spec       spillwayv1alpha1.LogPipelineSpec `json:"spec"`
}

func lastGoodSpec(p *spillwayv1alpha1.LogPipeline) (goodSpec, bool) {
	var g goodSpec
	raw, ok := p.Annotations[LastGoodSpecAnnotation]
	if !ok || json.Unmarshal([]byte(raw), &g) != nil {
		return g, false
	}
	return g, true
}

// recordLastGoodSpecs stores each pipeline's spec on it once that spec runs
// on every aggregator.
func (r *AggregatorReconciler) recordLastGoodSpecs(ctx context.Context, pipelines []spillwayv1alpha1.LogPipeline) error {
	for _, p := range pipelines {
		var lp spillwayv1alpha1.LogPipeline
		if err := r.Get(ctx, client.ObjectKeyFromObject(&p), &lp); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if lp.Generation != p.Generation {
			continue // a quarantined pipeline running its last good spec, or changed since
		}
		raw, err := json.Marshal(goodSpec{Generation: p.Generation, Spec: p.Spec})
		if err != nil {
			return err
		}
		if lp.Annotations[LastGoodSpecAnnotation] == string(raw) {
			continue
		}
		patch := client.MergeFrom(lp.DeepCopy())
		if lp.Annotations == nil {
			lp.Annotations = map[string]string{}
		}
		lp.Annotations[LastGoodSpecAnnotation] = string(raw)
		if err := r.Patch(ctx, &lp, patch); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *AggregatorReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// validated renders accepted and checks the config with Vector. If Vector
// rejects it, each pipeline is checked on its own: those Vector rejects alone
// are moved to invalid, and the rest are rendered and checked again. It
// returns the config to apply and the pipelines in it, or a *validate.Error
// if the remaining pipelines still don't validate together.
func (r *AggregatorReconciler) validated(ctx context.Context, accepted []spillwayv1alpha1.LogPipeline,
	invalid map[types.UID]string) ([]byte, []spillwayv1alpha1.LogPipeline, error) {
	cfg, err := render.Render(accepted, r.Options)
	if err != nil {
		// partition already rendered this exact set successfully.
		return nil, nil, fmt.Errorf("rendering accepted pipelines: %w", err)
	}
	err = r.Validator.Validate(ctx, cfg)
	var rejected *validate.Error
	if !errors.As(err, &rejected) {
		return cfg, accepted, err
	}

	var keep []spillwayv1alpha1.LogPipeline
	for _, p := range accepted {
		solo, err := render.Render([]spillwayv1alpha1.LogPipeline{p}, r.Options)
		if err != nil {
			return nil, nil, err
		}
		err = r.Validator.Validate(ctx, solo)
		var bad *validate.Error
		switch {
		case errors.As(err, &bad):
			invalid[p.UID] = "rendered config rejected by " + bad.Error()
		case err != nil:
			return nil, nil, err
		default:
			keep = append(keep, p)
		}
	}
	if len(keep) == len(accepted) {
		return nil, nil, rejected
	}
	if cfg, err = render.Render(keep, r.Options); err != nil {
		return nil, nil, err
	}
	return cfg, keep, r.Validator.Validate(ctx, cfg)
}

// partition splits pipelines into those that render and those that don't.
// Pipelines are taken oldest first, so when two conflict (the same team or
// namespace) the one that was there first keeps it and the newer one is
// marked Invalid.
func (r *AggregatorReconciler) partition(items []spillwayv1alpha1.LogPipeline) (accepted []spillwayv1alpha1.LogPipeline, invalid map[types.UID]string) {
	pipelines := make([]spillwayv1alpha1.LogPipeline, 0, len(items))
	for _, p := range items {
		if p.DeletionTimestamp == nil {
			pipelines = append(pipelines, p)
		}
	}
	sort.SliceStable(pipelines, func(i, j int) bool {
		a, b := pipelines[i], pipelines[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
	})
	invalid = map[types.UID]string{}
	for _, p := range pipelines {
		candidate := append(accepted, p)
		if _, err := render.Render(candidate, r.Options); err != nil {
			invalid[p.UID] = err.Error()
			continue
		}
		accepted = candidate
	}
	return accepted, invalid
}

// applyConfigMap writes the config for hash as its own file in the
// aggregator's ConfigMap and prunes revisions no pod or rollout needs: it keeps
// the bootstrap config, the desired, stable and canary revisions, and the one
// the pod template points at.
func (r *AggregatorReconciler) applyConfigMap(ctx context.Context, sts *appsv1.StatefulSet, hash string, cfg []byte) error {
	keep := map[string]bool{BootstrapKey: true, configKey(hash): true}
	for _, h := range []string{
		sts.Annotations[annStable], sts.Annotations[annCanary],
		strings.TrimPrefix(sts.Spec.Template.Annotations[SuffixAnnotation], "-"),
	} {
		if h != "" {
			keep[configKey(h)] = true
		}
	}

	var cm corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.ConfigMap}, &cm)
	if apierrors.IsNotFound(err) {
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: r.Namespace, Name: r.ConfigMap,
				Labels: map[string]string{"app.kubernetes.io/managed-by": "spillway-operator"}},
			Data: map[string]string{configKey(hash): string(cfg)},
		}
		return r.Create(ctx, &cm)
	} else if err != nil {
		return err
	}
	patch := client.MergeFrom(cm.DeepCopy())
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	changed := cm.Data[configKey(hash)] != string(cfg)
	cm.Data[configKey(hash)] = string(cfg)
	for k := range cm.Data {
		if !keep[k] {
			delete(cm.Data, k)
			changed = true
		}
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "spillway-operator" {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels["app.kubernetes.io/managed-by"] = "spillway-operator"
		changed = true
	}
	if !changed {
		return nil
	}
	return r.Patch(ctx, &cm, patch)
}

// report sets every pipeline's Ready condition. Invalid pipelines get their
// error, and quarantined ones (failed) why their change was held back. A pipeline whose current generation runs in the stable config is
// Ready; the rest get the rollout's phase. A non-empty reason overrides the
// phase (for problems with the aggregator itself).
func (r *AggregatorReconciler) report(ctx context.Context, items []spillwayv1alpha1.LogPipeline,
	invalid, failed map[types.UID]string, st rolloutState, reason, message string) {
	logger := log.FromContext(ctx)
	for i := range items {
		p := &items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		cond := metav1.Condition{Type: spillwayv1alpha1.ConditionReady, ObservedGeneration: p.Generation,
			Status: metav1.ConditionFalse, Reason: reason, Message: message}
		switch msg, bad := invalid[p.UID]; {
		case bad:
			cond.Reason, cond.Message = spillwayv1alpha1.ReasonInvalid, msg
		case failed[p.UID] != "":
			cond.Reason, cond.Message = spillwayv1alpha1.ReasonCanaryFailed, failed[p.UID]
		case reason != "":
		case inStable(st, p):
			cond.Status, cond.Reason = metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut
			cond.Message = "the aggregator is running config that includes this pipeline"
		case st.phase == phaseRejected:
			cond.Reason = spillwayv1alpha1.ReasonCanaryFailed
		default:
			cond.Reason = spillwayv1alpha1.ReasonRollingOut
		}
		if cond.Message == "" {
			cond.Message = "waiting for the aggregator"
		}
		patch := client.MergeFrom(p.DeepCopy())
		changed := meta.SetStatusCondition(&p.Status.Conditions, cond)
		if p.Status.ObservedGeneration != p.Generation {
			p.Status.ObservedGeneration = p.Generation
			changed = true
		}
		if !changed {
			continue
		}
		if err := r.Status().Patch(ctx, p, patch); err != nil && !apierrors.IsNotFound(err) {
			// The next reconcile retries; one pipeline's status shouldn't block the rest.
			logger.Error(err, "updating status", "pipeline", client.ObjectKeyFromObject(p))
		}
	}
}

// inStable reports whether the stable config contains p's current generation.
func inStable(st rolloutState, p *spillwayv1alpha1.LogPipeline) bool {
	gen, ok := st.stable[string(p.UID)]
	return ok && gen == p.Generation
}

// SetupWithManager registers the reconciler. LogPipeline changes, and changes
// to the aggregator's ConfigMap or StatefulSet (rollout progress, or someone
// overwriting the config), all map to the same single request.
func (r *AggregatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	one := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: r.Namespace, Name: r.StatefulSet}}}
	})
	isAggregator := func(name string) predicate.Funcs {
		return predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetNamespace() == r.Namespace && o.GetName() == name
		})
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("aggregator").
		// Spec changes only: status updates bump neither generation nor config.
		Watches(&spillwayv1alpha1.LogPipeline{}, one, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.ConfigMap{}, one, builder.WithPredicates(isAggregator(r.ConfigMap))).
		Watches(&appsv1.StatefulSet{}, one, builder.WithPredicates(isAggregator(r.StatefulSet))).
		Complete(r)
}
