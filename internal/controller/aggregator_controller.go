// Package controller reconciles LogPipelines into running Vector config.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
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

const (
	// ConfigKey is the file name of the rendered config in the ConfigMap.
	ConfigKey = "vector.yaml"
	// HashAnnotation on the aggregator's pod template carries the hash of the
	// config it should run; changing it rolls the pods.
	HashAnnotation = "spillway.dev/config-hash"
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

	// The aggregator: its namespace, StatefulSet and config ConfigMap.
	Namespace   string
	StatefulSet string
	ConfigMap   string
}

// +kubebuilder:rbac:groups=spillway.dev,resources=logpipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=spillway.dev,resources=logpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace=vector,resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,namespace=vector,resources=statefulsets,verbs=get;list;watch;patch

// Reconcile renders, applies and rolls out the aggregator config.
func (r *AggregatorReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var list spillwayv1alpha1.LogPipelineList
	if err := r.List(ctx, &list); err != nil {
		return ctrl.Result{}, err
	}
	accepted, invalid := r.partition(list.Items)

	cfg, accepted, err := r.validated(ctx, accepted, invalid)
	var rejected *validate.Error
	if errors.As(err, &rejected) {
		// Not even the pipelines that pass alone combine into a config Vector
		// accepts. Apply nothing: the aggregator keeps its last valid config.
		logger.Info("rendered config failed validation; keeping the current config", "output", rejected.Output)
		r.setStatuses(ctx, list.Items, invalid, metav1.ConditionFalse, spillwayv1alpha1.ReasonValidationFailed,
			"the combined config failed validation, so the aggregator keeps its last valid config: "+rejected.Error())
		return ctrl.Result{}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}
	sum := sha256.Sum256(cfg)
	hash := hex.EncodeToString(sum[:])[:16]

	if err := r.applyConfigMap(ctx, cfg); err != nil {
		return ctrl.Result{}, err
	}

	var sts appsv1.StatefulSet
	err = r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.StatefulSet}, &sts)
	if apierrors.IsNotFound(err) {
		msg := fmt.Sprintf("aggregator StatefulSet %s/%s not found", r.Namespace, r.StatefulSet)
		r.setStatuses(ctx, list.Items, invalid, metav1.ConditionFalse, spillwayv1alpha1.ReasonAggregatorNotFound, msg)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if sts.Spec.Template.Annotations[HashAnnotation] != hash {
		logger.Info("rolling out new aggregator config", "hash", hash, "pipelines", len(accepted), "invalid", len(invalid))
		patch := client.MergeFrom(sts.DeepCopy())
		if sts.Spec.Template.Annotations == nil {
			sts.Spec.Template.Annotations = map[string]string{}
		}
		sts.Spec.Template.Annotations[HashAnnotation] = hash
		if err := r.Patch(ctx, &sts, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	if !rolledOut(&sts, hash) {
		r.setStatuses(ctx, list.Items, invalid, metav1.ConditionFalse, spillwayv1alpha1.ReasonRollingOut,
			"the aggregator is restarting with new config")
		// The StatefulSet watch requeues as pods become ready; this is a backstop.
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	r.setStatuses(ctx, list.Items, invalid, metav1.ConditionTrue, spillwayv1alpha1.ReasonRolledOut,
		"the aggregator is running config that includes this pipeline")
	return ctrl.Result{}, nil
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

func (r *AggregatorReconciler) applyConfigMap(ctx context.Context, cfg []byte) error {
	var cm corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.ConfigMap}, &cm)
	if apierrors.IsNotFound(err) {
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: r.Namespace, Name: r.ConfigMap},
			Data:       map[string]string{ConfigKey: string(cfg)},
		}
		cm.Labels = map[string]string{"app.kubernetes.io/managed-by": "spillway-operator"}
		return r.Create(ctx, &cm)
	} else if err != nil {
		return err
	}
	if cm.Data[ConfigKey] == string(cfg) && len(cm.Data) == 1 {
		return nil
	}
	patch := client.MergeFrom(cm.DeepCopy())
	cm.Data = map[string]string{ConfigKey: string(cfg)}
	if cm.Labels == nil {
		cm.Labels = map[string]string{}
	}
	cm.Labels["app.kubernetes.io/managed-by"] = "spillway-operator"
	return r.Patch(ctx, &cm, patch)
}

// rolledOut reports whether every aggregator pod runs the config with hash.
func rolledOut(sts *appsv1.StatefulSet, hash string) bool {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	s := sts.Status
	return sts.Spec.Template.Annotations[HashAnnotation] == hash &&
		s.ObservedGeneration >= sts.Generation &&
		s.UpdateRevision != "" && s.CurrentRevision == s.UpdateRevision &&
		s.UpdatedReplicas == replicas && s.ReadyReplicas == replicas
}

// setStatuses sets the Ready condition on every pipeline: invalid ones get
// their render error, the rest get status/reason/message.
func (r *AggregatorReconciler) setStatuses(ctx context.Context, items []spillwayv1alpha1.LogPipeline,
	invalid map[types.UID]string, status metav1.ConditionStatus, reason, message string) {
	logger := log.FromContext(ctx)
	for i := range items {
		p := &items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		cond := metav1.Condition{
			Type:               spillwayv1alpha1.ConditionReady,
			Status:             status,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: p.Generation,
		}
		if msg, bad := invalid[p.UID]; bad {
			cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, spillwayv1alpha1.ReasonInvalid, msg
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
